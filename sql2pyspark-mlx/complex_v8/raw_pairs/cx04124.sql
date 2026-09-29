WITH scoped_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type IS NOT NULL
      AND actor = 'agent'
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        UPPER(TRIM(event_type)) AS event_type_clean,
        CASE WHEN event_type IN ('escalated', 'reopened') THEN 'group_a' WHEN event_type = 'created' THEN 'group_b' ELSE 'other' END AS event_type_group,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM scoped_events
),
latest_per_key AS (
    -- Keep the latest row per actor, event_type_group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_clean,
        event_type_group,
        event_type_slug
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor, event_type_group ORDER BY event_at DESC, id DESC) AS recency_rank
        FROM annotated
    ) ranked
    WHERE recency_rank = 1
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count
    FROM latest_per_key s
)
-- Final projection
SELECT
    id,
    event_at,
    event_type,
    actor,
    event_type_clean,
    event_type_slug,
    event_count,
    ABS(event_count - 3) AS event_count_distance
FROM summarized_rows
ORDER BY actor DESC
