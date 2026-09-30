WITH cleaned_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE (event_type IN ('reopened', 'created') OR event_at < TIMESTAMP '2025-03-02 09:00:00')
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN days_since_event_at >= 490 THEN 'large' WHEN days_since_event_at >= 140 THEN 'medium' ELSE 'small' END AS days_since_event_at_tier,
        CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref,
        FLOOR(days_since_event_at / 100) * 100 AS days_since_event_at_bucket
    FROM cleaned_events
    WHERE actor <> 'system'
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        event_type,
        actor,
        days_since_event_at_tier,
        event_type_ref,
        CONCAT_WS(': ', actor, event_type_ref) AS actor_event_type_ref_label
    FROM tagged
    WHERE event_at >= TIMESTAMP '2025-02-06 11:00:00'
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(TRIM(event_type_ref)) AS event_type_ref_clean,
        SUBSTRING(TRIM(days_since_event_at_tier), 1, 4) AS days_since_event_at_tier_prefix,
        LOWER(REGEXP_REPLACE(TRIM(event_type_ref), ' ', '_')) AS event_type_ref_slug
    FROM enriched
),
enriched_2 AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN days_since_event_at_tier IN ('medium', 'large') THEN 'group_a' WHEN days_since_event_at_tier = 'small' THEN 'group_b' ELSE 'other' END AS days_since_event_at_tier_group,
        COALESCE(days_since_event_at_tier, 'UNKNOWN') AS days_since_event_at_tier_or_unknown,
        CONCAT_WS('-', days_since_event_at_tier, event_type) AS days_since_event_at_tier_event_type_label
    FROM scored
    WHERE days_since_event_at_tier_prefix IS NOT NULL
)
-- Final projection
SELECT
    id,
    event_type,
    actor,
    event_type_ref,
    actor_event_type_ref_label,
    event_type_ref_clean,
    event_type_ref_slug,
    days_since_event_at_tier_group,
    days_since_event_at_tier_or_unknown
FROM enriched_2
ORDER BY event_type_ref DESC, id
LIMIT 5
