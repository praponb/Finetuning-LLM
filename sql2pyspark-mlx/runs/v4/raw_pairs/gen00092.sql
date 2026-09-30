WITH base AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        duration_sec,
        MONTH(event_time) event_time_month
    FROM web_events
    WHERE page IS NOT NULL
      AND event_time >= '2023-12-22'
)
SELECT
    *,
    CUME_DIST() OVER (PARTITION BY event_type ORDER BY event_time_month DESC NULLS LAST) event_time_month_cume_dist,
    AVG(event_time_month) OVER (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) event_time_month_avg_5,
    STDDEV_SAMP(duration_sec) OVER (PARTITION BY session_id) session_id_stddev_samp_duration_sec,
    LEAD(event_time_month, 1) OVER (PARTITION BY page, event_type ORDER BY duration_sec, id) next_event_time_month,
    NTILE(4) OVER (PARTITION BY session_id ORDER BY event_time_month DESC, id) event_time_month_bucket
FROM base;
