SELECT
    page,
    CASE WHEN duration_sec >= 526 THEN 'high' WHEN duration_sec >= 143 THEN 'mid' ELSE 'low' END AS duration_sec_bucket,
    MIN(duration_sec) AS min_duration_sec,
    COUNT(*) AS row_count
FROM web_events
GROUP BY page, CASE WHEN duration_sec >= 526 THEN 'high' WHEN duration_sec >= 143 THEN 'mid' ELSE 'low' END
