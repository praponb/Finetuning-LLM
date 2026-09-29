SELECT
    *,
    AVG(duration_sec) OVER w_part AS running_avg_duration_sec,
    SUM(duration_sec) OVER w_part AS running_sum_duration_sec,
    MAX(duration_sec) OVER w_part AS running_max_duration_sec,
    NTILE(5) OVER (PARTITION BY MONTH(event_time), session_id ORDER BY duration_sec, id) AS duration_sec_bucket
FROM web_events
WINDOW w_part AS (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
