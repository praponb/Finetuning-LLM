SELECT s.browser, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(we.page))) AS page_values FROM web_events we RIGHT JOIN sessions s ON s.id = we.session_id GROUP BY s.browser ORDER BY browser DESC;
