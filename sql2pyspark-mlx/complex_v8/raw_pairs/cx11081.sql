WITH base_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label,
        CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-03-08 09:00:00'
      AND event_type = 'created'
),
qualified AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM base_events s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type = 'created'
              AND t.actor IN ('customer', 'agent')
        )
      AND s.event_type_ref IS NOT NULL
)
-- Subtotals with CUBE
SELECT
    ticket_id,
    actor,
    event_type,
    COUNT(*) FILTER (WHERE event_type = 'assigned') AS n_event_type_assigned,
    SUM(CASE WHEN actor <> 'customer' THEN 1 ELSE 0 END) AS n_actor_customer
FROM qualified
GROUP BY CUBE(ticket_id, actor, event_type)
ORDER BY ticket_id NULLS LAST, actor NULLS LAST, event_type NULLS LAST
