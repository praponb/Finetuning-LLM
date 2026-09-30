WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN actor IN ('system', 'agent') THEN 'group_a' WHEN actor = 'customer' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
    WHERE actor <> 'customer'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        actor_group,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY actor_group ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY actor_group ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at), actor ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at_2,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', event_at), actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at_2,
        ROW_NUMBER() OVER (ORDER BY event_at, id) AS seq_no,
        RANK() OVER (ORDER BY event_at, id) AS event_at_rank,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor_group ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        RANK() OVER w_actor_group_by_event_at_ties AS event_at_rank_2,
        NTILE(5) OVER w_actor_group_by_event_at AS event_at_quintile
    FROM base
    WINDOW w_actor_group_by_event_at_ties AS (PARTITION BY actor_group ORDER BY event_at), w_actor_group_by_event_at AS (PARTITION BY actor_group ORDER BY event_at, id)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    actor_group,
    last_known_event_at,
    minutes_since_prev_event_at,
    minutes_since_prev_event_at_2,
    last_known_event_at_2,
    seq_no,
    event_at_rank,
    event_at_rank_2,
    event_at_quintile,
    FLOOR(event_at_quintile / 2) * 2 AS event_at_quintile_bucket,
    ABS(seq_no - 4) AS seq_no_distance
FROM metrics
WHERE last_known_event_at >= TIMESTAMP '2025-01-19 11:00:00'
ORDER BY ticket_id, last_known_event_at
