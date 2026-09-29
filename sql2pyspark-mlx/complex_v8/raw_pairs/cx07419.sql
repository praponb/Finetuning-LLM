WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id >= 15
),
with_prev AS (
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 45 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        event_type,
        burst_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY event_type, burst_id
),
grouped AS (
    -- Group-level totals
    SELECT
        event_type,
        TO_DATE(session_end) AS session_end_day,
        burst_id,
        MIN(n_events) AS min_n_events,
        SUM(n_events) AS total_n_events,
        MIN(duration_min) AS min_duration_min,
        SUM(CASE WHEN event_type IN ('created', 'reopened') THEN n_events ELSE 0 END) AS n_events_event_type_in_created_reopen,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list
    FROM sessions
    GROUP BY 1, 2, 3
    HAVING SUM(duration_min) > 24.0
),
flattened AS (
    -- One row per event_type_list_item
    SELECT
        s.event_type,
        s.session_end_day,
        s.burst_id,
        s.min_n_events,
        s.total_n_events,
        s.min_duration_min,
        s.n_events_event_type_in_created_reopen,
        t.event_type_list_item
    FROM grouped s
    LATERAL VIEW OUTER EXPLODE(s.event_type_list) t AS event_type_list_item
)
SELECT
    event_type,
    session_end_day,
    burst_id,
    min_n_events,
    total_n_events,
    min_duration_min,
    n_events_event_type_in_created_reopen,
    event_type_list_item,
    CONCAT(event_type, '#', CAST(total_n_events AS STRING)) AS event_type_ref,
    LOWER(REGEXP_REPLACE(TRIM(event_type_list_item), ' ', '_')) AS event_type_list_item_slug
FROM flattened
ORDER BY event_type_list_item
