WITH cleaned AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM ticket_events
    WHERE actor = 'customer'
      AND ticket_id <= 57
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(event_at) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        LAG(event_at) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC NULLS FIRST, id) AS prev_event_at,
        LAST_VALUE(event_at) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        LAST_VALUE(event_at) OVER (PARTITION BY event_type ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_2
    FROM cleaned
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_slug,
        first_event_at,
        prev_event_at,
        last_event_at,
        last_event_at_2,
        CONCAT_WS('-', actor_slug, event_type) AS actor_slug_event_type_label,
        HOUR(first_event_at) AS first_event_at_hour,
        INITCAP(TRIM(actor_slug)) AS actor_slug_clean
    FROM metrics
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor_slug,
        first_event_at,
        prev_event_at,
        last_event_at,
        last_event_at_2,
        actor_slug_event_type_label,
        first_event_at_hour,
        actor_slug_clean,
        CASE WHEN event_type IN ('replied', 'assigned') THEN 'group_a' WHEN event_type = 'reopened' THEN 'group_b' ELSE 'other' END AS event_type_group,
        LENGTH(TRIM(actor)) AS actor_len,
        CONCAT(actor_slug_clean, '#', CAST(id AS STRING)) AS actor_slug_clean_ref
    FROM enriched
    WHERE actor_slug_event_type_label IS NOT NULL
)
SELECT
    COALESCE(event_type, 'ALL') AS event_type,
    first_event_at_hour,
    COALESCE(event_type_group, 'ALL') AS event_type_group,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS n_rows,
    PERCENTILE_APPROX(first_event_at_hour, 0.5) AS median_first_event_at_hour
FROM scored
GROUP BY ROLLUP(event_type, first_event_at_hour, event_type_group)
ORDER BY event_type NULLS LAST, first_event_at_hour NULLS LAST, event_type_group NULLS LAST
