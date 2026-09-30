WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix
    FROM ticket_events
    WHERE actor IN ('agent', 'system')
),
tagged AS (
    -- Derive helper columns
    SELECT *, TO_DATE(event_at) AS event_at_date
    FROM filtered
)
-- Subtotals with CUBE
SELECT
    event_type,
    ticket_id,
    actor,
    SUM(CASE WHEN actor = 'customer' THEN 1 ELSE 0 END) AS n_actor_customer,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(CASE WHEN actor IN ('customer', 'system') THEN 1 ELSE 0 END) AS n_actor_in_customer_system
FROM tagged
GROUP BY CUBE(event_type, ticket_id, actor)
ORDER BY event_type NULLS LAST, ticket_id NULLS LAST, actor NULLS LAST
