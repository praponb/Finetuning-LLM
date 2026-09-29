WITH base_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN actor IN ('agent', 'system') THEN 'group_a' WHEN actor = 'customer' THEN 'group_b' ELSE 'other' END AS actor_group,
        SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix
    FROM ticket_events
    WHERE actor NOT IN ('system', 'agent')
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (PARTITION BY actor_group, actor ORDER BY event_at ASC, id) AS event_at_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY actor_group, actor ORDER BY event_at ASC, id), 3) AS event_at_cume_dist,
        LAG(event_at) OVER (PARTITION BY actor, ticket_id ORDER BY event_at, id) AS prev_event_at,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        NTILE(3) OVER w_actor_ticket_id_by_event_at AS event_at_tercile,
        DENSE_RANK() OVER w_event_at_month_by_event_at_t AS event_at_dense_rank,
        RANK() OVER (PARTITION BY actor, ticket_id ORDER BY event_at) AS event_at_rank_3,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_actor_ticket_id_by_event_at)) / 60, 1) AS minutes_since_prev_event_at,
        NTILE(4) OVER w_actor_group_actor_by_event_a AS event_at_quartile
    FROM base_events
    WINDOW w_actor_group_actor_by_event_a AS (PARTITION BY actor_group, actor ORDER BY event_at ASC, id), w_actor_ticket_id_by_event_at AS (PARTITION BY actor, ticket_id ORDER BY event_at, id), w_event_at_month_by_event_at_t AS (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at DESC)
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        TO_DATE(prev_event_at) AS prev_event_at_date,
        ROUND(event_at_tercile / NULLIF(event_at_cume_dist, 0), 3) AS event_at_tercile_per_event_at_cume_dist,
        LOWER(TRIM(actor)) AS actor_clean
    FROM ranked
    WHERE event_at_quartile BETWEEN 4 AND 7
)
-- Final projection
SELECT
    id,
    actor,
    actor_group,
    event_type_prefix,
    event_at_rank,
    event_at_cume_dist,
    prev_event_at,
    first_known_event_at,
    event_at_tercile,
    event_at_dense_rank,
    minutes_since_prev_event_at,
    event_at_quartile,
    prev_event_at_date,
    event_at_tercile_per_event_at_cume_dist,
    COALESCE(event_at_dense_rank, 0) AS event_at_dense_rank_filled
FROM enriched
ORDER BY id DESC
