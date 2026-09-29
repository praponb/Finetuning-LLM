WITH scored AS (
    SELECT
        *,
        AVG(duration_sec) OVER (PARTITION BY page, session_id) AS page_avg_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time, id) AS seq_no
    FROM web_events
)
SELECT
    CASE WHEN page_avg_duration_sec >= 465 THEN 'HIGH' ELSE 'LOW' END AS page_avg_duration_sec_tier,
    SIZE(COLLECT_SET(page)) AS num_pages,
    COUNT(DISTINCT page) AS distinct_pages,
    SIZE(COLLECT_SET(page)) AS num_pages_2,
    APPROX_PERCENTILE(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles
FROM scored
GROUP BY CASE WHEN page_avg_duration_sec >= 465 THEN 'HIGH' ELSE 'LOW' END
ORDER BY num_pages_2 NULLS LAST;
