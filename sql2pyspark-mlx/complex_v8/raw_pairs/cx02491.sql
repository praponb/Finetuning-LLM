WITH valid AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor IS NOT NULL
      AND event_type IS NOT NULL
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_at,
        event_type,
        actor,
        LAG(event_at, 2) OVER w_actor_by_event_at AS prev2_event_at,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        RANK() OVER w_event_at_month_ticket_id_by AS event_at_rank,
        LEAD(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS next_event_at,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY MONTH(event_at), ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        NTILE(5) OVER (ORDER BY event_at DESC, id) AS event_at_quintile,
        LAG(event_at, 2) OVER (ORDER BY event_at DESC, id) AS prev2_event_at_2,
        LAST_VALUE(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        LAST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        DENSE_RANK() OVER w_event_at_month_ticket_id_by_3 AS event_at_dense_rank
    FROM valid
    WINDOW w_actor_by_event_at AS (PARTITION BY actor ORDER BY event_at, id), w_event_at_month_ticket_id_by AS (PARTITION BY MONTH(event_at), ticket_id ORDER BY event_at), w_event_at_month_ticket_id_by_3 AS (PARTITION BY MONTH(event_at), ticket_id ORDER BY event_at, id)
),
windowed_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_at,
        actor,
        second_event_at,
        event_at_rank,
        next_event_at,
        first_known_event_at,
        prev2_event_at_2,
        last_event_at,
        last_known_event_at,
        COUNT(*) OVER (PARTITION BY event_at_dense_rank) AS rows_in_event_at_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY event_at_dense_rank ORDER BY second_event_at DESC, id), 3) AS second_event_at_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY TO_DATE(next_event_at), event_at_quintile ORDER BY event_at_rank ASC, id) AS rn,
        LAG(event_at_dense_rank, 2) OVER w_event_at_dense_rank_by_secon AS prev2_event_at_dense_rank,
        FIRST_VALUE(event_at_rank) IGNORE NULLS OVER (PARTITION BY event_at_dense_rank ORDER BY second_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_rank
    FROM windowed
    WINDOW w_event_at_dense_rank_by_secon AS (PARTITION BY event_at_dense_rank ORDER BY second_event_at DESC, id)
)
SELECT
    id,
    actor,
    event_at_rank,
    next_event_at,
    first_known_event_at,
    prev2_event_at_2,
    last_event_at,
    last_known_event_at,
    rows_in_event_at_dense_rank,
    second_event_at_cume_dist,
    rn,
    prev2_event_at_dense_rank,
    CASE WHEN actor IN ('agent', 'system') THEN 'group_a' WHEN actor = 'customer' THEN 'group_b' ELSE 'other' END AS actor_group
FROM windowed_2
WHERE second_event_at_cume_dist > 0.2
ORDER BY event_at_rank DESC
