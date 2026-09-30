WITH cleaned_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('resolved', 'assigned') THEN 'group_a' WHEN event_type = 'created' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 8 AND 10
      AND actor IN ('agent', 'system')
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        t.channel AS ticket_channel,
        t.requester_id AS ticket_requester_id
    FROM cleaned_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
SELECT
    COALESCE(ticket_channel, 'ALL') AS ticket_channel,
    ticket_id,
    COALESCE(event_type, 'ALL') AS event_type,
    GROUPING(ticket_channel) AS is_total_ticket_channel,
    COUNT(*) AS row_count,
    MAX(ticket_requester_id) AS max_ticket_requester_id
FROM with_parent
GROUP BY CUBE(ticket_channel, ticket_id, event_type)
ORDER BY ticket_channel NULLS LAST, ticket_id NULLS LAST, event_type NULLS LAST
