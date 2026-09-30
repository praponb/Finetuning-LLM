SELECT
    CASE WHEN page IN ('/search', '/checkout') THEN 'tier_1' ELSE 'other' END AS page_group,
    SUM(duration_sec) AS total_duration_sec,
    COUNT(*) AS cnt,
    RANK() OVER (ORDER BY SUM(duration_sec) DESC) AS rnk
FROM web_events
WHERE event_time < '2023-11-01'
GROUP BY CASE WHEN page IN ('/search', '/checkout') THEN 'tier_1' ELSE 'other' END
ORDER BY total_duration_sec DESC, page_group DESC;
