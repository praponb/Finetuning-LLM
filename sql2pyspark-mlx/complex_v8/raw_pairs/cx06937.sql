WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM ticket_events
    WHERE event_type IN ('assigned', 'created', 'reopened')
      AND HOUR(event_at) BETWEEN 9 AND 15
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DENSE_RANK() OVER (ORDER BY event_at ASC) AS event_at_dense_rank,
        ROW_NUMBER() OVER (ORDER BY event_at ASC, id) AS event_at_seq,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY actor, event_type_or_unknown ORDER BY event_at DESC NULLS FIRST, id))) / 60, 1) AS minutes_since_prev_event_at,
        NTILE(4) OVER (PARTITION BY actor, event_type_or_unknown ORDER BY event_at DESC NULLS FIRST, id) AS event_at_quartile,
        LAG(event_at, 2) OVER (PARTITION BY actor, event_type_or_unknown ORDER BY event_at DESC NULLS FIRST, id) AS prev2_event_at,
        LEAD(event_at, 3) OVER (PARTITION BY event_type_or_unknown, ticket_id ORDER BY event_at, id) AS next3_event_at,
        DENSE_RANK() OVER (PARTITION BY event_type_or_unknown, ticket_id ORDER BY event_at) AS event_at_dense_rank_2,
        ROUND(CUME_DIST() OVER (PARTITION BY actor, event_type_or_unknown ORDER BY event_at DESC NULLS FIRST), 3) AS event_at_cume_dist,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY actor, event_type_or_unknown ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        DENSE_RANK() OVER (PARTITION BY actor, event_type_or_unknown ORDER BY event_at DESC NULLS FIRST, id) AS event_at_dense_rank_3,
        LAST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        LEAD(event_at) OVER w_actor_event_type_or_unknown AS next_event_at
    FROM base
    WINDOW w_event_type_or_unknown_ticket_2 AS (PARTITION BY event_type_or_unknown, ticket_id ORDER BY event_at), w_actor_event_type_or_unknown AS (PARTITION BY actor, event_type_or_unknown ORDER BY event_at DESC NULLS FIRST, id)
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM metrics
    WHERE event_at_dense_rank_2 <= 5
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY event_at_dense_rank_3 ORDER BY event_at_dense_rank_3, id) AS rn,
        NTILE(10) OVER w_event_at_dense_rank_3_by_eve AS event_at_dense_rank_3_decile,
        COUNT(*) OVER w_event_at_seq AS rows_in_event_at_seq
    FROM first_rows
    WINDOW w_event_at_dense_rank_3_by_eve AS (PARTITION BY event_at_dense_rank_3 ORDER BY event_at_dense_rank_3, id), w_event_at_seq AS (PARTITION BY event_at_seq)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    event_at_dense_rank,
    event_at_quartile,
    prev2_event_at,
    next3_event_at,
    event_at_dense_rank_2,
    event_at_cume_dist,
    first_known_event_at,
    event_at_dense_rank_3,
    last_known_event_at,
    next_event_at,
    rn,
    event_at_dense_rank_3_decile,
    rows_in_event_at_seq,
    ROUND(event_at_cume_dist * 1.07, 2) AS event_at_cume_dist_adj
FROM windowed
ORDER BY prev2_event_at DESC
