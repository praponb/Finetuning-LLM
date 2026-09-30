WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        LOWER(event_type) AS event_type_lower,
        COALESCE(page, 'UNKNOWN') AS page_filled
    FROM web_events
    WHERE event_time > TO_TIMESTAMP('2023-09-12 05:00:00')
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM base
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN duration_sec >= 401 THEN 'LARGE' ELSE 'SMALL' END AS duration_sec_tier,
    CASE WHEN burst_id >= 6 THEN 'HIGH' ELSE 'LOW' END AS burst_id_tier,
    DATE_TRUNC('WEEK', event_time) AS week_start,
    COUNT(CASE WHEN page = '/checkout' THEN 1 END) AS checkout_count,
    SIZE(COLLECT_SET(event_type)) AS num_event_types,
    BIT_AND(duration_sec) AS duration_sec_bit_and,
    COUNT_IF(page = '/cart') AS cart_events,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS span_minutes,
    GROUPING_ID() AS grouping_id
FROM sessions_web_events
GROUP BY ROLLUP (CASE WHEN duration_sec >= 401 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN burst_id >= 6 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('WEEK', event_time))
ORDER BY num_event_types DESC NULLS FIRST;
