SELECT
    DATE_TRUNC('WEEK', started_at) AS week_start,
    DATE_TRUNC('YEAR', started_at) AS year_start,
    COUNT(*) FILTER (WHERE browser = 'Safari') AS safari_rows,
    SORT_ARRAY(COLLECT_SET(device)) AS device_list,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min,
    COUNT(CASE WHEN country = 'US' THEN 1 END) AS us_count,
    MAX(started_at) AS last_seen,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', started_at) ORDER BY (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 DESC) AS duration_min_dense_rank
FROM sessions
GROUP BY week_start, year_start
ORDER BY us_count NULLS FIRST;
