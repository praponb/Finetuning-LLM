WITH base_events AS (
    -- Filter ticket_events
    SELECT *
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        event_type,
        actor,
        LENGTH(TRIM(event_type)) AS event_type_len,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at
    FROM base_events
    WHERE ticket_id >= 19
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(event_type_len - 10) AS event_type_len_distance,
        CASE WHEN event_type IN ('created', 'resolved') THEN 'group_a' WHEN event_type = 'reopened' THEN 'group_b' ELSE 'other' END AS event_type_group,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM scored
    WHERE actor IS NOT NULL
)
-- Final projection
SELECT
    id,
    actor,
    event_type_len,
    event_type_len_distance,
    event_type_group,
    actor_slug,
    CASE WHEN event_type_len_distance >= 9 THEN 'A' WHEN event_type_len_distance >= 5 THEN 'B' ELSE 'C' END AS event_type_len_distance_tier
FROM labeled
WHERE actor = 'agent'
ORDER BY event_type_group DESC, event_type_len
