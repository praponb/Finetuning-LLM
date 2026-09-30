WITH prepared_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        UPPER(TRIM(actor)) AS actor_clean
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-12 08:00:00'
),
joined AS (
    -- Join tickets
    SELECT
        s.*,
        t.opened_at AS ticket_opened_at,
        t.channel AS ticket_channel,
        t.closed_at AS ticket_closed_at
    FROM prepared_events s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND t.opened_at < TIMESTAMP '2025-03-02 08:00:00'
)
-- Subtotals with CUBE
SELECT
    COALESCE(event_type, 'ALL') AS event_type,
    COALESCE(ticket_channel, 'ALL') AS ticket_channel,
    GROUPING(event_type) AS is_total_event_type,
    COUNT(DISTINCT ticket_channel) AS distinct_ticket_channels
FROM joined
GROUP BY CUBE(event_type, ticket_channel)
ORDER BY event_type NULLS LAST, ticket_channel NULLS LAST
