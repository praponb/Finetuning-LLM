WITH base_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-25 13:00:00'
      AND ticket_id >= 51
),
with_activity_2 AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM base_events s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.channel IN ('web', 'email', 'phone')
        )
)
-- Subtotals with ROLLUP
SELECT
    event_type,
    ticket_id,
    actor,
    SUM(CASE WHEN actor IN ('customer', 'system') THEN 1 ELSE 0 END) AS n_actor_in_customer_system,
    COUNT(*) FILTER (WHERE actor NOT IN ('customer', 'system')) AS n_actor_not_in_customer_system,
    COUNT(*) FILTER (WHERE event_type IS NOT NULL) AS n_event_type_is_not_null
FROM with_activity_2
GROUP BY ROLLUP(event_type, ticket_id, actor)
