WITH filtered_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        CASE WHEN actor IN ('system', 'agent') THEN 'group_a' WHEN actor = 'customer' THEN 'group_b' ELSE 'other' END AS actor_group,
        LOWER(TRIM(event_type)) AS event_type_clean
    FROM ticket_events
    WHERE MONTH(event_at) IN (2, 3)
      AND actor IN ('agent', 'system')
),
summary AS (
    -- Summarise per group
    SELECT
        actor,
        MIN(event_at) AS first_event_at,
        COUNT(DISTINCT actor) AS distinct_actors,
        COUNT(*) FILTER (WHERE actor IN ('agent', 'customer')) AS n_actor_in_agent_customer,
        MAX(event_at) AS last_event_at,
        COUNT(DISTINCT event_type) AS distinct_event_types
    FROM filtered_events
    GROUP BY actor
)
-- Native SQL UNPIVOT (wide to long)
SELECT actor, measure, reading
FROM (
    SELECT actor, n_actor_in_agent_customer, distinct_event_types, distinct_actors
    FROM summary
) src
UNPIVOT (
    reading FOR measure IN (n_actor_in_agent_customer, distinct_event_types, distinct_actors)
) unp
