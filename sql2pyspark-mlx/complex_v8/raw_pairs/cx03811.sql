WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 120 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        event_type,
        burst_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(days_since_event_at) AS session_days_since_event_at,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY event_type, burst_id
),
per_group AS (
    -- Summarise per group
    SELECT
        n_events,
        COUNT(*) FILTER (WHERE event_type = 'resolved') AS n_event_type_resolved,
        COUNT(*) AS n_rows,
        COUNT(*) FILTER (WHERE event_type = 'reopened') AS n_event_type_reopened,
        ROUND(100 * COUNT(CASE WHEN event_type IN ('escalated', 'resolved', 'reopened') THEN 1 END) / COUNT(*), 1) AS pct_event_type_in_escalated_reso,
        COUNT(DISTINCT event_type) AS distinct_event_types,
        MIN(session_end) AS first_session_end
    FROM per_session
    GROUP BY 1
)
-- Final projection
SELECT
    n_events,
    n_event_type_resolved,
    n_rows,
    n_event_type_reopened,
    pct_event_type_in_escalated_reso,
    distinct_event_types,
    first_session_end,
    ABS(n_events - 3) AS n_events_distance,
    DATEDIFF(DATE '2025-12-31', first_session_end) AS days_since_first_session_end
FROM per_group
ORDER BY first_session_end, n_event_type_reopened NULLS LAST
