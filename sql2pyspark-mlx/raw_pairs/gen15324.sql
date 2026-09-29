SELECT
    customer_id,
    DATE_TRUNC('MONTH', started_at) AS activity_month,
    MAX(started_at) AS last_started_at,
    COUNT(CASE WHEN country = 'JP' THEN 1 END) AS jp_count,
    SORT_ARRAY(COLLECT_SET(browser)) AS browser_list,
    MAX(started_at) AS ended_at,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min,
    COUNT(*) FILTER (WHERE country = 'US') AS us_rows
FROM sessions
GROUP BY customer_id, activity_month
