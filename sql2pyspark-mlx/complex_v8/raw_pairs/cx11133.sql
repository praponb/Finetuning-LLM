WITH filtered_events AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
annotated AS (
    -- Derive helper columns
    SELECT *, DATE_TRUNC('DAY', event_at) AS event_at_day
    FROM filtered_events
    WHERE event_type = 'replied'
),
joined_ticket AS (
    -- Join tickets
    SELECT s.*, t.closed_at AS ticket_closed_at, t.agent_id AS ticket_agent_id
    FROM annotated s
    JOIN tickets t ON t.id = s.ticket_id
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    ticket_closed_at,
    ticket_agent_id,
    ticket_closed_at + INTERVAL 7 DAYS AS ticket_closed_at_plus_7d
FROM joined_ticket
