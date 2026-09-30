WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        YEAR(event_at) AS event_at_year,
        SUBSTRING(TRIM(event_type), 1, 3) AS event_type_prefix,
        SUBSTRING(TRIM(actor), 1, 2) AS actor_prefix
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-02-24'
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        actor,
        event_type_prefix,
        actor_prefix,
        CONCAT(actor_prefix, '#', CAST(ticket_id AS STRING)) AS actor_prefix_ref
    FROM filtered
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(event_type_prefix), ' ', '_')) AS event_type_prefix_slug,
        CASE WHEN actor IN ('customer', 'system') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM tagged
    WHERE actor_prefix_ref IS NOT NULL
)
-- Final projection
SELECT
    id,
    actor,
    event_type_prefix,
    actor_prefix,
    actor_prefix_ref,
    SUBSTRING(TRIM(event_type_prefix_slug), 1, 4) AS event_type_prefix_slug_prefix
FROM annotated
ORDER BY actor_prefix_ref NULLS LAST
