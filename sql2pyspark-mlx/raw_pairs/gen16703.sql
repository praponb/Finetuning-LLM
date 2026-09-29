WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        CAST(event_time AS DATE) AS event_time_day
    FROM web_events
    WHERE event_type IN ('page_view', 'purchase')
),
filtered_web_events AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LENGTH(event_type) AS event_type_length
    FROM filtered
    WHERE event_time > TIMESTAMP '2023-06-07 17:00:00'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        duration_sec,
        event_type_length,
        SUM(duration_sec) OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
        SUM(event_type_length) OVER (PARTITION BY MONTH(event_time_day), event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_event_type_length,
        LAG(event_type_length, 2) OVER (ORDER BY event_time_day, id) AS prev_event_type_length,
        ROW_NUMBER() OVER (ORDER BY duration_sec DESC, id) AS event_seq,
        NTILE(4) OVER (PARTITION BY page, event_type ORDER BY duration_sec DESC, id) AS duration_sec_bucket
    FROM filtered_web_events
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) AS prev_event_time
    FROM enriched
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL '14' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        session_id,
        event_time,
        duration_sec,
        event_type_length,
        event_seq,
        SUM(new_group_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM with_gaps
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM sessions_web_events
),
sessions_web_events_2 AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN event_type_length >= 74 THEN 'HIGH' ELSE 'LOW' END AS event_type_length_tier,
    DATE_TRUNC('YEAR', event_time) AS year_start,
    DATE_TRUNC('MONTH', event_time) AS activity_month,
    BIT_OR(duration_sec) AS duration_sec_bit_or,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS span_minutes,
    MIN(event_seq) AS min_event_seq
FROM sessions_web_events_2
GROUP BY CASE WHEN event_type_length >= 74 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('YEAR', event_time), DATE_TRUNC('MONTH', event_time)
ORDER BY duration_sec_bit_or NULLS FIRST;
