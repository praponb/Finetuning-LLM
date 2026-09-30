WITH in_window AS (
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
exploded_detail_tickets AS (
    -- One row per ticket
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        t.id AS ticket_id_2,
        t.requester_id AS ticket_requester_id,
        t.closed_at AS ticket_closed_at
    FROM in_window s
    LEFT JOIN tickets t ON t.agent_id = s.agent_id AND t.requester_id <= 17
)
SELECT
    id,
    ticket_id,
    agent_id,
    hours_since_open,
    ticket_requester_id,
    ticket_closed_at
FROM exploded_detail_tickets
WHERE MONTH(event_at) IN (1, 2)
ORDER BY hours_since_open DESC
