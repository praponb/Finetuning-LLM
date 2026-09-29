WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at_2,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM ticket_events
    WHERE event_type = 'escalated'
      AND ticket_id >= 54
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(event_at) OVER (PARTITION BY event_type, ticket_id ORDER BY days_since_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        LEAD(days_since_event_at) OVER (PARTITION BY event_type, ticket_id ORDER BY days_since_event_at DESC, id) AS next_days_since_event_at
    FROM prepared
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY actor, event_type_len ORDER BY days_since_event_at_2 ASC NULLS LAST, id) AS days_since_event_at_2_tercile,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY event_type)) AS distinct_actor_in_event_type
    FROM with_trends
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(event_type_len / NULLIF(SUM(event_type_len) OVER (PARTITION BY actor, event_type), 0), 4) AS event_type_len_share_of_actor_event_type,
        COUNT(*) OVER (PARTITION BY actor, event_type) AS rows_in_actor_event_type
    FROM ranked
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT event_type_len, ticket_id, event_type, next_days_since_event_at
    FROM windowed
) src
PIVOT (
    MIN(next_days_since_event_at)
    FOR event_type IN ('reopened', 'created', 'assigned', 'resolved')
)
