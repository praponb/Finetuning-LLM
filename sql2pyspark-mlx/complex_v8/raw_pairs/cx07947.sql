WITH base_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM base_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
screened AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM sessionized s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
            WHERE t.event_type <> 'replied'
              AND TO_DATE(t.event_at) < DATE '2025-02-18'
        )
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_type,
        prev_event_at,
        starts_session,
        burst_id,
        NTH_VALUE(event_at, 2) OVER (ORDER BY starts_session DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND(CUME_DIST() OVER (ORDER BY starts_session DESC), 3) AS starts_session_cume_dist,
        COUNT(*) OVER w_event_type_2 AS rows_in_event_type,
        LAG(event_at) OVER (ORDER BY starts_session DESC, id) AS prev_event_at_2
    FROM screened
    WINDOW w_event_type_2 AS (PARTITION BY event_type)
)
SELECT
    id,
    event_type,
    prev_event_at,
    starts_session,
    burst_id,
    second_event_at,
    starts_session_cume_dist,
    rows_in_event_type,
    prev_event_at_2,
    COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
FROM sequenced
WHERE burst_id >= 2
ORDER BY id DESC
