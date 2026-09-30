WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        MONTH(event_at) AS event_at_month
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-13 14:00:00'
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
with_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.event_at_month,
        s.prev_event_at,
        s.new_session_flag,
        s.session_id,
        t.opened_at AS ticket_opened_at
    FROM sessionized s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND t.csat < 5
),
enriched_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.event_at_month,
        s.prev_event_at,
        s.new_session_flag,
        s.session_id,
        s.ticket_opened_at,
        t.requester_id AS ticket_requester_id
    FROM with_details_ticket s
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_at_month,
    new_session_flag,
    session_id,
    CONCAT(actor, '#', CAST(new_session_flag AS STRING)) AS actor_ref,
    LEAST(new_session_flag, ticket_requester_id) AS least_new_session_flag_ticket_requester_id
FROM enriched_ticket
ORDER BY new_session_flag, event_at NULLS LAST, id
LIMIT 20
