WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type IN ('escalated', 'replied', 'reopened')
      AND event_at >= TIMESTAMP '2025-01-25 13:00:00'
),
labeled AS (
    -- Label and score rows
    SELECT *, LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM base
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY event_at), 3) AS event_at_cume_dist,
        LAG(event_at, 3) OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id) AS prev3_event_at
    FROM labeled
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        event_at,
        actor,
        event_type_slug,
        prev3_event_at,
        ROUND(AVG(event_at_cume_dist) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at_cume_dist, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS event_at_cume_dist_avg_4,
        COUNT(*) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM with_windows
)
SELECT
    id,
    event_at,
    event_type_slug,
    prev3_event_at,
    event_at_cume_dist_avg_4,
    running_count
FROM ranked
ORDER BY running_count DESC
