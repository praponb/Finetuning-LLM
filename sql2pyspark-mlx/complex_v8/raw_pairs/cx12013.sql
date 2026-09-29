WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        CASE WHEN actor IN ('customer', 'system') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
    WHERE ticket_id >= 24
),
per_group AS (
    -- Group-level totals
    SELECT
        event_type_or_unknown,
        actor_group,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(100 * COUNT(CASE WHEN actor IN ('agent', 'customer') THEN 1 END) / COUNT(*), 1) AS pct_actor_in_agent_customer
    FROM recent
    GROUP BY event_type_or_unknown, actor_group
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM per_group
    WHERE distinct_ids > (SELECT MIN(distinct_ids) FROM per_group)
)
SELECT
    event_type_or_unknown,
    actor_group,
    distinct_ids,
    pct_actor_in_agent_customer
FROM qualified
WHERE actor_group = 'group_b'
ORDER BY actor_group, distinct_ids DESC
