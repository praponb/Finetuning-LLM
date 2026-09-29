WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(TRIM(event_type)) AS event_type_clean,
        CASE WHEN actor IN ('system', 'customer') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
    WHERE event_type = 'escalated'
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        event_at,
        event_type,
        actor,
        event_type_clean,
        actor_group,
        CONCAT_WS('-', actor_group, event_type) AS actor_group_event_type_label,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM scoped
),
enriched AS (
    -- Add calculated columns
    SELECT *, MONTH(event_at) AS event_at_month
    FROM labeled
    WHERE UPPER(TRIM(event_type_clean)) LIKE 'C%'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        event_at,
        event_type,
        actor,
        event_type_clean,
        actor_group,
        actor_group_event_type_label,
        actor_slug,
        event_at_month,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY actor_group)) AS distinct_event_type_in_actor_group,
        DENSE_RANK() OVER (ORDER BY event_at NULLS LAST) AS event_at_dense_rank,
        ROUND(CUME_DIST() OVER (ORDER BY event_at NULLS LAST, id), 3) AS event_at_cume_dist
    FROM enriched
)
SELECT
    id,
    event_at,
    actor,
    event_type_clean,
    actor_group_event_type_label,
    event_at_month,
    distinct_event_type_in_actor_group,
    INITCAP(TRIM(actor_slug)) AS actor_slug_clean
FROM ranked
ORDER BY actor NULLS LAST, event_at_month DESC, id
LIMIT 20
