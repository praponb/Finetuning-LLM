WITH valid AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at + INTERVAL 2 HOURS AS event_at_plus_2h
    FROM ticket_events
    WHERE ticket_id >= 54
      AND actor IN ('agent', 'customer')
)
SELECT ticket_id AS ref_id, event_type
FROM valid
UNION ALL
SELECT requester_id, priority
FROM tickets
WHERE channel = 'web'
ORDER BY ref_id DESC, event_type
