WITH cleaned_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor = 'system'
),
combined AS (
    -- Stack the branches
    SELECT actor, id AS qty, event_at AS event_time
    FROM cleaned_events
    WHERE ticket_id >= 35
    UNION ALL
    SELECT actor, id, event_at
    FROM ticket_events
    UNION ALL
    SELECT channel, agent_id, opened_at
    FROM tickets
)
SELECT actor, COUNT(*) AS n_rows, SUM(qty) AS total_qty, MAX(event_time) AS latest_event_time
FROM combined
GROUP BY actor
ORDER BY actor
