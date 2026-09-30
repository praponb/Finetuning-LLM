WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(': ', actor, event_type) AS actor_event_type_label,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND actor = 'system'
),
screened AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type <> 'assigned'
        )
)
-- Subtotals with CUBE
SELECT
    actor,
    event_type,
    GROUPING(actor) AS is_total_actor,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(days_since_event_at), 2) AS avg_days_since_event_at
FROM screened
GROUP BY CUBE(actor, event_type)
