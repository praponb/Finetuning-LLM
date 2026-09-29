WITH valid_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM valid_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 30 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        event_type,
        burst_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY event_type, burst_id
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', session_start) AS session_start_month,
        YEAR(session_end) AS session_end_year,
        SUM(events) AS total_events,
        COUNT(*) AS cnt,
        COUNT(DISTINCT event_type) AS distinct_event_types,
        MAX(events) AS max_events,
        SUM(burst_id) AS total_burst_id
    FROM session_summary
    WHERE events > 2
    GROUP BY 1, 2
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE distinct_event_types > (SELECT AVG(distinct_event_types) FROM stats)
)
-- Native SQL UNPIVOT (wide to long)
SELECT session_start_month, session_end_year, metric, reading
FROM (
    SELECT session_start_month, session_end_year, distinct_event_types, cnt, total_burst_id
    FROM qualified
) src
UNPIVOT (
    reading FOR metric IN (distinct_event_types, cnt, total_burst_id)
) unp
