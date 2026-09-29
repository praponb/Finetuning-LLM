WITH prepared AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
joined AS (
    -- Attach ticket attributes
    SELECT s.*, t.closed_at AS ticket_closed_at
    FROM prepared s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND t.opened_at >= TIMESTAMP '2025-01-13 13:00:00'
),
with_activity AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM joined s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor IS NOT NULL
              AND t.event_type = 'resolved'
        )
)
SELECT
    ticket_id,
    COALESCE(actor, 'ALL') AS actor,
    COALESCE(event_type, 'ALL') AS event_type,
    GROUPING(ticket_id) AS is_total_ticket_id,
    SUM(CASE WHEN actor = 'system' THEN 1 ELSE 0 END) AS n_actor_system,
    SUM(CASE WHEN actor <> 'customer' THEN 1 ELSE 0 END) AS n_actor_customer
FROM with_activity
GROUP BY GROUPING SETS ((ticket_id, actor, event_type), (ticket_id))
ORDER BY ticket_id NULLS LAST, actor NULLS LAST, event_type NULLS LAST
