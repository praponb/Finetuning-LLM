SELECT
    *,
    STDDEV_SAMP(duration_sec) OVER w_running session_id_stddev_samp_duration_sec,
    ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time DESC, id) event_seq,
    LAG(duration_sec, 2) OVER (PARTITION BY event_type, page ORDER BY duration_sec, id) prev_duration_sec,
    SUM(duration_sec) OVER w_grp cumulative_duration_sec
FROM web_events
WINDOW w_running AS (PARTITION BY session_id), w_grp AS (PARTITION BY TO_DATE(event_time), event_type ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
