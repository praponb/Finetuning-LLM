WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM ticket_events
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        event_type,
        actor,
        event_type_slug,
        SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix
    FROM scoped_events
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        event_type,
        actor,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT event_type) AS distinct_event_types,
        COUNT(*) AS cnt,
        COUNT(*) FILTER (WHERE actor NOT IN ('system', 'agent')) AS n_actor_not_in_system_agent,
        SUM(CASE WHEN event_type IN ('created', 'replied', 'escalated') THEN 1 ELSE 0 END) AS n_event_type_in_created_replie
    FROM scored
    GROUP BY event_type, actor
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT cnt, n_event_type_in_created_replie, event_type, distinct_event_types
    FROM totals
) src
PIVOT (
    COUNT(distinct_event_types)
    FOR event_type IN ('escalated', 'resolved', 'created', 'replied')
)
ORDER BY cnt, n_event_type_in_created_replie
