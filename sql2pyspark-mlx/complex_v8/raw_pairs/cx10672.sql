WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE MONTH(event_at) IN (2, 3)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        event_type,
        actor,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        SUM(CASE WHEN event_type = 'escalated' THEN 1 ELSE 0 END) AS n_event_type_escalated,
        COUNT(DISTINCT event_type) AS distinct_event_types
    FROM filtered
    GROUP BY event_type, actor
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(event_type, '#', CAST(n_event_type_escalated AS STRING)) AS event_type_ref
    FROM rollup_base
),
exploded AS (
    -- One row per actor_list_item
    SELECT
        s.event_type,
        s.actor,
        s.n_event_type_escalated,
        s.distinct_event_types,
        s.event_type_ref,
        t.actor_list_item
    FROM tagged s
    LATERAL VIEW EXPLODE(s.actor_list) t AS actor_list_item
),
derived AS (
    -- Derive helper columns
    SELECT
        event_type,
        actor,
        event_type_ref,
        actor_list_item,
        ROUND(n_event_type_escalated / NULLIF(distinct_event_types, 0), 3) AS n_event_type_escalated_per_distinct_event_types,
        CASE WHEN event_type IN ('replied', 'escalated') THEN 'group_a' WHEN event_type = 'resolved' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM exploded
    WHERE actor IN ('system', 'customer')
),
derived_2 AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN actor IN ('system', 'customer') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM derived
),
derived_3 AS (
    -- Derive helper columns
    SELECT
        event_type,
        actor,
        actor_list_item,
        n_event_type_escalated_per_distinct_event_types,
        actor_group,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM derived_2
    WHERE event_type_group IS NOT NULL
)
-- Final projection
SELECT
    event_type,
    actor,
    n_event_type_escalated_per_distinct_event_types,
    actor_group,
    event_type_or_unknown
FROM derived_3
ORDER BY actor_group DESC NULLS FIRST, actor
