import os
import json
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError
from bs4 import BeautifulSoup

# === CONFIG ===
BASE_URL = "https://discourse.onlinedegree.iitm.ac.in"
CATEGORY_ID = 34
CATEGORY_JSON_URL = f"{BASE_URL}/c/courses/tds-kb/{CATEGORY_ID}.json"
AUTH_STATE_FILE = "auth.json"
OUTPUT_DIR = "downloaded_threads"
DATE_FROM = datetime(2025, 1, 1)
DATE_TO = datetime(2025, 4, 14)

def parse_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")

def login_and_save_auth(playwright):
    print("🔐 No auth found. Launching browser for manual login...")
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()
    page.goto(f"{BASE_URL}/login")
    print("🌐 Please log in manually using Google. Then press ▶️ (Resume) in Playwright bar.")
    page.pause()
    context.storage_state(path=AUTH_STATE_FILE)
    print("✅ Login state saved.")
    browser.close()

def is_authenticated(page):
    try:
        page.goto(CATEGORY_JSON_URL, timeout=10000)
        page.wait_for_selector("pre", timeout=5000)
        json.loads(page.inner_text("pre"))
        return True
    except (TimeoutError, json.JSONDecodeError):
        return False

def scrape_posts(playwright):
    print("🔍 Starting scrape using saved session...")
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context(storage_state=AUTH_STATE_FILE)
    page = context.new_page()

    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_topics = []
    page_num = 0
    while True:
        paginated_url = f"{CATEGORY_JSON_URL}?page={page_num}"
        print(f"📦 Fetching page {page_num}...")
        page.goto(paginated_url)

        try:
            data = json.loads(page.inner_text("pre"))
        except:
            data = json.loads(page.content())

        topics = data.get("topic_list", {}).get("topics", [])
        if not topics:
            break

        all_topics.extend(topics)
        page_num += 1

    print(f"📄 Found {len(all_topics)} total topics across all pages")

    all_posts = []  # Still collect all posts for the combined file
    processed_topics = 0
    
    for topic in all_topics:
        created_at = parse_date(topic["created_at"])
        if DATE_FROM <= created_at <= DATE_TO:
            processed_topics += 1
            print(f"📖 Processing topic {processed_topics}: {topic.get('title', 'Untitled')}")
            
            topic_url = f"{BASE_URL}/t/{topic['slug']}/{topic['id']}.json"
            try:
                page.goto(topic_url)
                topic_data = json.loads(page.inner_text("pre"))
            except:
                try:
                    topic_data = json.loads(page.content())
                except Exception as e:
                    print(f"❌ Error loading topic {topic['id']}: {e}")
                    continue

            posts = topic_data.get("post_stream", {}).get("posts", [])
            if not posts:
                continue
                
            accepted_answer_id = topic_data.get("accepted_answer", topic_data.get("accepted_answer_post_id"))

            # Build reply count map
            reply_counter = {}
            for post in posts:
                reply_to = post.get("reply_to_post_number")
                if reply_to is not None:
                    reply_counter[reply_to] = reply_counter.get(reply_to, 0) + 1

            # Process posts for this topic
            topic_posts = []
            for post in posts:
                try:
                    # Extract and clean content
                    raw_content = post.get("cooked", "")
                    clean_content = BeautifulSoup(raw_content, "html.parser").get_text().strip()
                    
                    # Skip empty posts
                    if not clean_content:
                        continue
                    
                    # Create consistent post structure
                    post_data = {
                        "topic_id": topic["id"],
                        "topic_title": topic.get("title", ""),
                        "category_id": topic.get("category_id"),
                        "tags": topic.get("tags", []),
                        "post_id": post["id"],
                        "post_number": post["post_number"],
                        "author": post.get("username", "unknown"),
                        "created_at": post.get("created_at", ""),
                        "updated_at": post.get("updated_at", ""),
                        "reply_to_post_number": post.get("reply_to_post_number"),
                        "is_reply": post.get("reply_to_post_number") is not None,
                        "reply_count": reply_counter.get(post["post_number"], 0),
                        "like_count": post.get("like_count", 0),
                        "is_accepted_answer": post["id"] == accepted_answer_id if accepted_answer_id else False,
                        "mentioned_users": [u.get("username", "") for u in post.get("mentioned_users", [])],
                        "url": f"{BASE_URL}/t/{topic['slug']}/{topic['id']}/{post['post_number']}",
                        "content": clean_content
                    }
                    
                    topic_posts.append(post_data)
                    all_posts.append(post_data)  # Also add to combined list
                    
                except Exception as e:
                    print(f"⚠️ Error processing post {post.get('id', 'unknown')}: {e}")
                    continue

            # Save individual topic file
            if topic_posts:
                topic_filename = f"topic_{topic['id']}.json"
                topic_filepath = os.path.join(OUTPUT_DIR, topic_filename)
                
                # Create topic metadata
                topic_data_to_save = {
                    "topic_metadata": {
                        "topic_id": topic["id"],
                        "title": topic.get("title", ""),
                        "slug": topic.get("slug", ""),
                        "category_id": topic.get("category_id"),
                        "tags": topic.get("tags", []),
                        "created_at": topic.get("created_at", ""),
                        "posts_count": len(topic_posts),
                        "views": topic.get("views", 0),
                        "like_count": topic.get("like_count", 0),
                        "url": f"{BASE_URL}/t/{topic.get('slug', '')}/{topic['id']}"
                    },
                    "posts": topic_posts
                }
                
                with open(topic_filepath, "w", encoding="utf-8") as f:
                    json.dump(topic_data_to_save, f, indent=2, ensure_ascii=False)
                
                print(f"💾 Saved topic {topic['id']} with {len(topic_posts)} posts to {topic_filename}")

    # Save combined file (all posts)
    combined_output_file = os.path.join(OUTPUT_DIR, "discourse_posts.json")
    with open(combined_output_file, "w", encoding="utf-8") as f:
        json.dump(all_posts, f, indent=2, ensure_ascii=False)

    # Create summary file
    summary = {
        "scrape_metadata": {
            "scraped_at": datetime.now().isoformat(),
            "date_range": {
                "from": DATE_FROM.isoformat(),
                "to": DATE_TO.isoformat()
            },
            "total_topics": processed_topics,
            "total_posts": len(all_posts),
            "category_id": CATEGORY_ID,
            "base_url": BASE_URL
        },
        "files_created": {
            "combined_posts": "discourse_posts.json",
            "individual_topics": [f"topic_{topic['id']}.json" for topic in all_topics 
                                if DATE_FROM <= parse_date(topic["created_at"]) <= DATE_TO]
        }
    }
    
    summary_file = os.path.join(OUTPUT_DIR, "scrape_summary.json")
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    print(f"✅ Scraped {len(all_posts)} posts from {processed_topics} topics")
    print(f"📁 Combined data saved to: {combined_output_file}")
    print(f"📁 Individual topics saved as: topic_[ID].json")
    print(f"📁 Summary saved to: {summary_file}")
    print(f"📅 Date range: {DATE_FROM.date()} to {DATE_TO.date()}")
    
    browser.close()

def main():
    with sync_playwright() as p:
        if not os.path.exists(AUTH_STATE_FILE):
            login_and_save_auth(p)
        else:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(storage_state=AUTH_STATE_FILE)
            page = context.new_page()
            if not is_authenticated(page):
                print("⚠️ Session invalid. Re-authenticating...")
                browser.close()
                login_and_save_auth(p)
            else:
                print("✅ Using existing authenticated session.")
                browser.close()

        scrape_posts(p)

if __name__ == "__main__":
    main()