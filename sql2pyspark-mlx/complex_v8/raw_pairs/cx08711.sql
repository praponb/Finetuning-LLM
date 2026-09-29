WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DAYOFWEEK(event_at) AS event_at_dayofweek
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        event_at_dayofweek,
        ABS(event_at_dayofweek - 5) AS event_at_dayofweek_distance,
        SUBSTRING(TRIM(event_type), 1, 2) AS event_type_prefix
    FROM recent
    WHERE actor = 'agent'
)
-- Subtotals with CUBE
SELECT
    ticket_id,
    COALESCE(actor, 'ALL') AS actor,
    GROUPING_ID() AS grouping_level,
    SUM(CASE WHEN actor IN ('agent', 'customer') THEN 1 ELSE 0 END) AS n_actor_in_agent_customer
FROM annotated
GROUP BY CUBE(ticket_id, actor)
ORDER BY ticket_id NULLS LAST, actor NULLS LAST
