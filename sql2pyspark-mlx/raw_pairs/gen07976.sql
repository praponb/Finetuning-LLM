SELECT we.event_time, we.event_type, we.page, s.browser, INITCAP(TRIM(we.page)) AS page_title FROM web_events we RIGHT JOIN sessions s ON we.session_id = s.id ORDER BY page ASC
