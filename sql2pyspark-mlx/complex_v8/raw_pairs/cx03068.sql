WITH base_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN actor IN ('system', 'agent') THEN 'group_a' WHEN actor = 'customer' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
    WHERE ticket_id <= 51
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        LEAD(event_at, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at DESC, id) AS next2_event_at,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        ROW_NUMBER() OVER w_ticket_id_by_event_at AS event_at_seq,
        LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id) AS prev_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_actor_group_event_at_month_b)) / 60, 1) AS minutes_since_prev_event_at_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_id ORDER BY event_at ASC), 3) AS event_at_percent_rank,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at DESC) AS event_at_rank,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        RANK() OVER (PARTITION BY ticket_id ORDER BY event_at ASC) AS event_at_rank_2,
        LAST_VALUE(event_at) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        DENSE_RANK() OVER (PARTITION BY actor_group, DATE_TRUNC('MONTH', event_at) ORDER BY event_at ASC) AS event_at_dense_rank,
        RANK() OVER (PARTITION BY actor_group, DATE_TRUNC('MONTH', event_at) ORDER BY event_at ASC, id) AS event_at_rank_3,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id), 3) AS event_at_cume_dist
    FROM base_events
    WINDOW w_ticket_id_by_event_at AS (PARTITION BY ticket_id ORDER BY event_at ASC, id), w_actor_group_event_at_month_b AS (PARTITION BY actor_group, DATE_TRUNC('MONTH', event_at) ORDER BY event_at ASC, id)
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM ranked
    WHERE event_at_seq = 1
      AND HOUR(next2_event_at) BETWEEN 10 AND 16
),
ranked_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_type,
        next2_event_at,
        event_at_seq,
        minutes_since_prev_event_at,
        minutes_since_prev_event_at_2,
        event_at_percent_rank,
        second_event_at,
        event_at_rank_2,
        DENSE_RANK() OVER (ORDER BY event_at_seq) AS event_at_seq_dense_rank,
        SUM(minutes_since_prev_event_at) OVER (ORDER BY event_at_seq, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS minutes_since_prev_event_at_sum_4,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY event_at_rank_2, DATE_TRUNC('MONTH', last_event_at))) AS distinct_actor_in_event_at_rank_2_last_event_at_,
        ROUND(event_at_rank - AVG(event_at_rank) OVER (PARTITION BY event_at_rank_2, DATE_TRUNC('MONTH', last_event_at)), 2) AS event_at_rank_vs_event_at_rank_2_last_event_at_m,
        ROW_NUMBER() OVER (ORDER BY event_at_seq, id) AS rn,
        COUNT(*) OVER (PARTITION BY event_at_rank_2, DATE_TRUNC('MONTH', last_event_at)) AS rows_in_event_at_rank_2_last_event_at_month
    FROM first_rows
)
SELECT
    id,
    ticket_id,
    next2_event_at,
    event_at_seq,
    minutes_since_prev_event_at,
    event_at_percent_rank,
    second_event_at,
    event_at_rank_2,
    event_at_seq_dense_rank,
    minutes_since_prev_event_at_sum_4,
    distinct_actor_in_event_at_rank_2_last_event_at_,
    event_at_rank_vs_event_at_rank_2_last_event_at_m,
    rn
FROM ranked_2
WHERE distinct_actor_in_event_at_rank_2_last_event_at_ < 6
ORDER BY minutes_since_prev_event_at_sum_4, id
LIMIT 5
