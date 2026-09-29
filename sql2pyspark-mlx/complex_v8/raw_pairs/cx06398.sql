WITH scoped AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-02-06'
      AND actor <> 'agent'
),
joined_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        t.tags AS ticket_tags,
        t.channel AS ticket_channel
    FROM scoped s
    JOIN tickets t ON t.id = s.ticket_id
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at
    FROM joined_ticket s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
SELECT
    COALESCE(ticket_channel, 'ALL') AS ticket_channel,
    ticket_id,
    MAX(last_event_at) AS last_last_event_at
FROM profiled
GROUP BY GROUPING SETS ((ticket_channel, ticket_id), (ticket_channel), ())
ORDER BY ticket_channel NULLS LAST, ticket_id NULLS LAST
