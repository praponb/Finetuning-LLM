SELECT CASE WHEN page IN ('/search', '/checkout') THEN 'tier_1' ELSE 'other' END page_group, COUNT(*) total_count, ROUND((100 * SUM(CASE WHEN page = '/search' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_search
FROM web_events
WHERE event_type = 'search'
GROUP BY 1
ORDER BY pct_search;
