SELECT s.country, ROUND((100 * SUM(CASE WHEN we.event_type = 'page_view' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_page_view, SUM(CASE WHEN we.page = '/cart' THEN 1 ELSE 0 END) AS cart_count
FROM web_events we
JOIN sessions s ON we.session_id = s.id
GROUP BY s.country
ORDER BY pct_page_view DESC, country;
