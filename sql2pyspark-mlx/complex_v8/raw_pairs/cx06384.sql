WITH scoped AS (
    -- Scope the ticket_events rows
    SELECT *
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        event_at,
        actor,
        CASE WHEN event_type IN ('reopened', 'created') THEN 'group_a' WHEN event_type = 'resolved' THEN 'group_b' ELSE 'other' END AS event_type_group,
        SUBSTRING(TRIM(event_type), 1, 3) AS event_type_prefix
    FROM scoped
    WHERE event_at >= TIMESTAMP '2025-01-31 13:00:00'
),
latest_per_key AS (
    -- Keep the earliest row per event_type_group
    SELECT
        id,
        event_at,
        actor,
        event_type_group,
        event_type_prefix
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type_group ORDER BY event_at, id) AS rn
        FROM enriched
    ) ranked
    WHERE rn = 1
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        actor,
        event_type_prefix,
        COALESCE(event_type_group, 'UNKNOWN') AS event_type_group_or_unknown
    FROM latest_per_key
    WHERE MONTH(event_at) IN (1, 3)
)
SELECT
    event_type_group_or_unknown,
    actor,
    COUNT(DISTINCT actor) AS distinct_actors,
    COUNT(*) FILTER (WHERE actor NOT IN ('agent', 'system')) AS n_actor_not_in_agent_system
FROM labeled
GROUP BY GROUPING SETS ((event_type_group_or_unknown, actor), (event_type_group_or_unknown))
ORDER BY event_type_group_or_unknown NULLS LAST, actor NULLS LAST
