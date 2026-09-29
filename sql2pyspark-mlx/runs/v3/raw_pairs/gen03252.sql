SELECT page, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) AS page_values
FROM web_events
WHERE event_type = 'search' AND page IN ('/search', '/home') AND page IN ('/cart', '/search')
GROUP BY page
