WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        TO_DATE(event_at) AS event_at_date,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM ticket_events
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(event_type_slug, '#', CAST(ticket_id AS STRING)) AS event_type_slug_ref,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug_2,
        CASE WHEN actor IN ('agent', 'customer') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM valid
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        event_at_date,
        actor_group,
        NTILE(10) OVER (PARTITION BY YEAR(event_at), event_type ORDER BY event_at_date, id) AS event_at_date_decile,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_event_at_year_event_type_by)) / 60, 1) AS minutes_since_prev_event_at,
        LAG(event_at_date) OVER w_event_at_year_event_type_by AS prev_event_at_date
    FROM tagged
    WINDOW w_event_at_year_event_type_by AS (PARTITION BY YEAR(event_at), event_type ORDER BY event_at_date, id)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_at_date,
    actor_group,
    event_at_date_decile,
    minutes_since_prev_event_at,
    prev_event_at_date,
    COALESCE(event_at_date_decile, 0) AS event_at_date_decile_filled
FROM metrics
ORDER BY event_at, id
LIMIT 5
