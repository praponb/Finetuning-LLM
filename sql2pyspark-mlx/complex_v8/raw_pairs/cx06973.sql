WITH cleaned AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 120 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
joined_ticket AS (
    -- Join tickets
    SELECT s.*, t.closed_at AS ticket_closed_at, t.agent_id AS ticket_agent_id
    FROM sessionized s
    JOIN tickets t ON t.id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    prev_event_at,
    burst_id,
    ticket_closed_at,
    ticket_agent_id,
    CASE WHEN burst_id >= 5 THEN 'large' WHEN burst_id >= 3 THEN 'medium' ELSE 'small' END AS burst_id_tier,
    UPPER(TRIM(event_type)) AS event_type_clean
FROM joined_ticket
