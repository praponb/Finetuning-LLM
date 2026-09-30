WITH scoped_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-18 11:00:00'
),
with_prev AS (
    -- Previous event_at per ticket_id
    SELECT *, LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev_event_at
    FROM scoped_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 45 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        session_id,
        CASE WHEN session_id >= 4 THEN 'large' WHEN session_id >= 2 THEN 'medium' ELSE 'small' END AS session_id_tier,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        GREATEST(session_id, is_new_session) AS greatest_session_id_is_new_session
    FROM sessionized
    WHERE actor = 'agent'
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    session_id,
    session_id_tier,
    actor_slug,
    greatest_session_id_is_new_session,
    COALESCE(session_id, 0) AS session_id_filled,
    DATE_TRUNC('MONTH', event_at) AS event_at_month
FROM scored
