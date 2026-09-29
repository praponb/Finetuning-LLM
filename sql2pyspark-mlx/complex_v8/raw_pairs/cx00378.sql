WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE (actor IS NOT NULL OR TO_DATE(event_at) < DATE '2025-02-24')
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        ROUND(CUME_DIST() OVER w_event_type_by_event_at_ties, 3) AS event_at_cume_dist,
        LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at DESC NULLS FIRST, id) AS prev_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY actor_or_unknown, ticket_id ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at,
        RANK() OVER (ORDER BY event_at DESC) AS event_at_rank,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        LAST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        DENSE_RANK() OVER w_actor_or_unknown_ticket_id_b_2 AS event_at_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_at DESC NULLS FIRST, id) AS rn,
        NTILE(10) OVER (PARTITION BY actor_or_unknown, ticket_id ORDER BY event_at, id) AS event_at_decile,
        FIRST_VALUE(event_at) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        FIRST_VALUE(event_at) OVER (PARTITION BY event_type ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at_2,
        DENSE_RANK() OVER w_actor_or_unknown_ticket_id_b AS event_at_dense_rank_2
    FROM recent
    WINDOW w_event_type_by_event_at_ties AS (PARTITION BY event_type ORDER BY event_at DESC NULLS FIRST), w_actor_or_unknown_ticket_id_b AS (PARTITION BY actor_or_unknown, ticket_id ORDER BY event_at, id), w_actor_or_unknown_ticket_id_b_2 AS (PARTITION BY actor_or_unknown, ticket_id ORDER BY event_at)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_type,
        actor_or_unknown,
        first_known_event_at,
        event_at_cume_dist,
        prev_event_at,
        minutes_since_prev_event_at,
        event_at_rank,
        second_event_at,
        event_at_dense_rank,
        first_event_at,
        first_event_at_2,
        event_at_dense_rank_2,
        NTILE(3) OVER (PARTITION BY event_at_decile, actor_or_unknown ORDER BY second_event_at DESC, id) AS second_event_at_tercile,
        ROUND(event_at_rank / NULLIF(SUM(event_at_rank) OVER (PARTITION BY event_at_decile), 0), 4) AS event_at_rank_share_of_event_at_decile,
        MAX(event_at_rank) OVER (PARTITION BY event_at_decile) AS event_at_decile_max_event_at_rank
    FROM with_trends
)
SELECT
    id,
    ticket_id,
    event_type,
    actor_or_unknown,
    event_at_cume_dist,
    prev_event_at,
    minutes_since_prev_event_at,
    event_at_rank,
    second_event_at,
    event_at_dense_rank,
    first_event_at,
    event_at_dense_rank_2,
    second_event_at_tercile,
    event_at_rank_share_of_event_at_decile,
    event_at_decile_max_event_at_rank,
    COALESCE(event_at_cume_dist, 0.0) AS event_at_cume_dist_filled,
    event_at_rank + second_event_at_tercile AS event_at_rank_plus_second_event_at_tercile
FROM metrics
WHERE first_event_at >= TIMESTAMP '2025-01-31 13:00:00'
ORDER BY first_event_at DESC NULLS FIRST, actor_or_unknown
