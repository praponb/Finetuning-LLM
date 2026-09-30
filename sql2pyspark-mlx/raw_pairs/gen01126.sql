WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DAY(event_time) AS event_time_day,
        COALESCE(page, 'UNKNOWN') AS page_clean
    FROM web_events
    WHERE event_time > TIMESTAMP '2023-07-19 20:00:00'
      AND page IS NOT NULL
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_TRUNC('year', event_time) AS event_time_period,
        COALESCE(page, 'OTHER') AS resolved_page
    FROM filtered
    WHERE event_time > TO_DATE('2023-10-16')
      AND page IS NOT NULL
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM base
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time_period) OVER (PARTITION BY event_type ORDER BY event_time_period, id) IS NULL
                 OR event_time_period > LAG(event_time_period) OVER (PARTITION BY event_type ORDER BY event_time_period, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM sessionized
),
sessionized_web_events AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        event_type,
        event_time,
        page_clean,
        event_time_period,
        resolved_page,
        session_start_flag,
        burst_id,
        SUM(is_new_burst) OVER (PARTITION BY event_type ORDER BY event_time_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps_2
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time_period - LAG(event_time_period) OVER (PARTITION BY event_type ORDER BY event_time_period, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM sessionized_web_events
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY event_type ORDER BY event_time_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT
    id,
    event_type,
    page_clean,
    event_time_period,
    resolved_page,
    session_start_flag,
    burst_id,
    is_new_burst,
    session_id
FROM sessions_web_events
ORDER BY session_start_flag DESC NULLS FIRST, id DESC
LIMIT 20;
