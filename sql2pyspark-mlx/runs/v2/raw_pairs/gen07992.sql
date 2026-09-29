SELECT s.browser, s.country, we.page, INITCAP(TRIM(s.browser)) AS browser_title FROM sessions s LEFT JOIN web_events we ON we.session_id = s.id ORDER BY we.page;
