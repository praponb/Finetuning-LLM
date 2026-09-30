WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type IS NOT NULL
      AND ticket_id <= 16
),
combined AS (
    -- Stack the branches
    SELECT id, ticket_id AS key_id
    FROM prepared
    UNION ALL
    SELECT id, ticket_id AS key_id
    FROM ticket_events
    WHERE event_type = 'resolved'
)
SELECT id, COUNT(*) AS n_rows, SUM(key_id) AS total_key_id
FROM combined
GROUP BY id
ORDER BY id
