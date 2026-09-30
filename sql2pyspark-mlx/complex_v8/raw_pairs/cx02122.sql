WITH base_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at + INTERVAL 30 MINUTES AS event_at_plus_30m,
        SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix,
        CONCAT_WS(': ', event_type, actor) AS event_type_actor_label
    FROM ticket_events
    WHERE ticket_id <= 50
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        event_type,
        actor,
        event_type_actor_label,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM base_events
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(event_type_actor_label), 1, 4) AS event_type_actor_label_prefix
    FROM scored
    WHERE actor_slug IS NOT NULL
),
scored_2 AS (
    -- Add calculated columns
    SELECT
        *,
        INITCAP(TRIM(event_type_actor_label)) AS event_type_actor_label_clean
    FROM derived
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        actor,
        event_type_actor_label,
        actor_slug,
        event_type_actor_label_prefix,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        LOWER(REGEXP_REPLACE(TRIM(event_type_actor_label), ' ', '_')) AS event_type_actor_label_slug,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM scored_2
)
-- Final projection
SELECT
    id,
    event_type_actor_label,
    actor_slug,
    event_type_actor_label_prefix,
    event_type_or_unknown,
    event_type_actor_label_slug,
    COALESCE(actor, 'UNKNOWN') AS actor_or_unknown_2,
    LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug_2
FROM enriched
WHERE event_type_or_unknown IN ('resolved', 'reopened', 'created')
ORDER BY actor_or_unknown_2 DESC NULLS FIRST, actor_slug NULLS LAST, id
LIMIT 10
