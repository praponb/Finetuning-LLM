WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor = 'customer'
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        DATE_TRUNC('HOUR', event_at) AS event_at_hour,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown,
        CASE WHEN actor IN ('customer', 'agent') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM prepared
    WHERE actor <> 'customer'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY event_at DESC), 3) AS event_at_cume_dist,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at,
        LAST_VALUE(event_at) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at
    FROM enriched
),
annotated AS (
    -- Label and score rows
    SELECT *, ABS(event_at_cume_dist - 0.5) AS event_at_cume_dist_distance
    FROM metrics
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_at_hour,
    actor_or_unknown,
    actor_group,
    minutes_since_prev_event_at
FROM annotated
WHERE actor_or_unknown <> 'UNKNOWN'
ORDER BY ticket_id DESC
