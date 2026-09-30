WITH metrics AS (
    SELECT
        *,
        MAX(duration_sec) OVER (PARTITION BY session_id) AS session_id_max_duration_sec,
        CUME_DIST() OVER (PARTITION BY MONTH(event_time), event_type ORDER BY duration_sec NULLS LAST) AS duration_sec_cume_dist,
        NTILE(5) OVER (PARTITION BY session_id, page ORDER BY duration_sec, id) AS duration_sec_bucket,
        NTILE(4) OVER (PARTITION BY page ORDER BY duration_sec DESC, id) AS duration_sec_bucket_2,
        SUM(duration_sec) OVER (PARTITION BY event_type) AS event_type_sum_duration_sec
    FROM web_events
)
SELECT
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    MAX(event_time) AS last_event_time,
    COUNT(DISTINCT id) AS distinct_ids,
    MAX(event_time) AS last_event_time_2
FROM metrics
GROUP BY CUBE (DATE_TRUNC('QUARTER', event_time))
ORDER BY quarter_start NULLS FIRST;
