WITH valid_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        SIZE(tags) AS tags_count
    FROM tickets
    WHERE MONTH(closed_at) IN (1, 3)
)
SELECT
    priority,
    channel AS category,
    opened_at,
    'ticket' AS origin
FROM valid_tickets
WHERE ARRAY_CONTAINS(tags, 'refund')
  AND closed_at >= TIMESTAMP '2025-01-16 00:00:00'
UNION ALL
SELECT
    actor,
    event_type,
    event_at,
    'event' AS origin
FROM ticket_events
UNION ALL
SELECT
    event_type,
    actor,
    event_at,
    'event' AS origin
FROM ticket_events
