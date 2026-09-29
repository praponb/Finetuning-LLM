SELECT
    QUARTER(event_time) AS event_quarter,
    page,
    COUNT(CASE WHEN page = '/home' THEN 1 END) AS home_count,
    COUNT(*) AS cnt
FROM web_events
GROUP BY QUARTER(event_time), page
HAVING COUNT(*) > 2
ORDER BY page
