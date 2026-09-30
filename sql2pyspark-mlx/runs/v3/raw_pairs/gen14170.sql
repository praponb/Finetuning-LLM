SELECT duration_sec, page, INITCAP(TRIM(page)) AS page_title FROM web_events WHERE page = '/product' ORDER BY duration_sec DESC, page
