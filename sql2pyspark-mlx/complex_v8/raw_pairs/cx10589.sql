WITH cleaned_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND actor IS NOT NULL
),
with_parent_ticket AS (
    -- Attach ticket attributes
    SELECT s.*, t.closed_at AS ticket_closed_at, t.agent_id AS ticket_agent_id
    FROM cleaned_events s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.agent_id <= 9
),
detail_rows AS (
    -- One row per ticket
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.ticket_agent_id,
        t.id AS ticket_id_2,
        t.closed_at AS ticket_closed_at_2,
        t.csat AS ticket_csat,
        t.channel AS ticket_channel
    FROM with_parent_ticket s
    JOIN tickets t ON t.agent_id = s.ticket_agent_id
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    ticket_id_2,
    ticket_closed_at_2,
    ticket_csat,
    ticket_channel
FROM detail_rows
