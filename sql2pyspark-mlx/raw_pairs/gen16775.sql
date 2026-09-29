WITH with_windows AS (
    SELECT
        *,
        AVG(duration_sec) OVER w_part AS last4_avg_duration_sec,
        MIN(duration_sec) OVER w_part AS last4_min_duration_sec,
        COUNT(duration_sec) OVER w_part AS last4_count_duration_sec,
        AVG(duration_sec) OVER (ORDER BY CAST(event_time AS BIGINT) ASC RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS duration_sec_avg_7d
    FROM web_events
    WINDOW w_part AS (PARTITION BY page ORDER BY event_time ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
SELECT
    event_type,
    session_id,
    DATE_TRUNC('MONTH', event_time) AS activity_month,
    MAX(event_time) AS last_event_time,
    BIT_AND(duration_sec) AS duration_sec_bit_and,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS span_minutes
FROM with_windows
GROUP BY event_type, session_id, DATE_TRUNC('MONTH', event_time)
