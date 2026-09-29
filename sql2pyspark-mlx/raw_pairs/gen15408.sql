WITH with_windows AS (
    SELECT
        id,
        session_id,
        event_type,
        duration_sec,
        ROW_NUMBER() OVER (PARTITION BY YEAR(event_time), page ORDER BY duration_sec, id) AS row_seq,
        SUM(duration_sec) OVER w_part AS event_time_year_sum_duration_sec,
        NTILE(3) OVER (ORDER BY duration_sec, id) AS duration_sec_bucket,
        ROW_NUMBER() OVER (ORDER BY duration_sec DESC, id) AS row_seq_2,
        LEAD(duration_sec, 2) OVER (PARTITION BY event_type ORDER BY duration_sec, id) AS next_duration_sec
    FROM web_events
    WINDOW w_part AS (PARTITION BY YEAR(event_time), session_id)
)
SELECT
    CASE WHEN next_duration_sec >= 427 THEN 'LARGE' ELSE 'SMALL' END AS next_duration_sec_tier,
    APPROX_PERCENTILE(row_seq, 0.95) AS row_seq_p95,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
    SIZE(COLLECT_SET(event_type)) AS num_event_types,
    APPROX_PERCENTILE(next_duration_sec, 0.95) AS next_duration_sec_p95
FROM with_windows
GROUP BY CASE WHEN next_duration_sec >= 427 THEN 'LARGE' ELSE 'SMALL' END
HAVING row_seq_p95 < 1000000
ORDER BY num_event_types
