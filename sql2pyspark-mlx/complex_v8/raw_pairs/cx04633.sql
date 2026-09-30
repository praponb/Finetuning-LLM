WITH matched AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 1 DAYS)
),
with_children_tickets AS (
    -- One row per ticket
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        t.id AS ticket_id_2,
        t.requester_id AS ticket_requester_id,
        t.opened_at AS ticket_opened_at,
        t.channel AS ticket_channel
    FROM matched s
    LEFT JOIN tickets t ON t.agent_id = s.agent_id
),
deduped AS (
    -- Keep the earliest row per event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        priority,
        agent_id,
        hours_since_open,
        ticket_id_2,
        ticket_requester_id,
        ticket_opened_at,
        ticket_channel
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY ticket_requester_id, id, ticket_id_2) AS rn
        FROM with_children_tickets
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    ticket_id,
    event_type,
    priority,
    agent_id,
    hours_since_open,
    ticket_id_2,
    ticket_requester_id,
    ticket_opened_at,
    ROUND(hours_since_open * 1.07, 2) AS hours_since_open_adj
FROM deduped
ORDER BY id NULLS LAST
