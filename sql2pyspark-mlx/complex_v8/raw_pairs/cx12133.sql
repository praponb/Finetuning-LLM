WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(event_at) OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        RANK() OVER (ORDER BY event_at, id) AS event_at_rank,
        ROW_NUMBER() OVER (ORDER BY event_at, id) AS seq_no,
        LEAD(event_at) OVER (PARTITION BY actor ORDER BY event_at ASC, id) AS next_event_at,
        FIRST_VALUE(event_at) OVER (PARTITION BY event_type, TO_DATE(event_at) ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at_2,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        RANK() OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id) AS event_at_rank_2,
        LAG(event_at, 3) OVER w_event_type_event_at_to_date_2 AS prev3_event_at,
        ROUND(CUME_DIST() OVER (ORDER BY event_at), 3) AS event_at_cume_dist,
        RANK() OVER (PARTITION BY event_type, actor ORDER BY event_at DESC) AS event_at_rank_3,
        RANK() OVER (PARTITION BY event_type, TO_DATE(event_at) ORDER BY event_at) AS event_at_rank_4
    FROM valid
    WINDOW w_actor_by_event_at AS (PARTITION BY actor ORDER BY event_at ASC, id), w_event_type_event_at_to_date_2 AS (PARTITION BY event_type, TO_DATE(event_at) ORDER BY event_at, id)
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        first_event_at,
        event_at_rank,
        next_event_at,
        second_event_at,
        event_at_rank_2,
        prev3_event_at,
        event_at_rank_3,
        event_at_rank_4,
        CASE WHEN actor IN ('customer', 'agent') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group,
        FLOOR(event_at_rank_4 / 2) * 2 AS event_at_rank_4_bucket
    FROM metrics
    WHERE event_at_rank > 5
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        event_at,
        actor,
        first_event_at,
        event_at_rank,
        event_at_rank_2,
        prev3_event_at,
        event_at_rank_3,
        event_at_rank_4_bucket,
        MAX(event_at_rank_2) OVER (PARTITION BY TO_DATE(prev3_event_at)) AS prev3_event_at_to_date_max_event_at_rank_2,
        DENSE_RANK() OVER w_actor_by_second_event_at AS second_event_at_dense_rank,
        SUM(event_at_rank_3) OVER w_event_type_event_at_rank_4_b AS event_type_event_at_rank_4_bucket_sum_event_at_r,
        COUNT(*) OVER (PARTITION BY event_type, event_at_rank_4_bucket) AS rows_in_event_type_event_at_rank_4_bucket,
        NTH_VALUE(event_at_rank_4, 2) OVER (PARTITION BY event_at_rank, event_at_rank_3 ORDER BY event_at_rank_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_rank_4,
        RANK() OVER w_all_by_next_event_at_ties AS next_event_at_rank,
        ROUND(AVG(event_at_rank) OVER (PARTITION BY actor ORDER BY second_event_at, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS event_at_rank_avg_2
    FROM labeled
    WINDOW w_actor_by_second_event_at AS (PARTITION BY actor ORDER BY second_event_at, id), w_event_type_event_at_rank_4_b AS (PARTITION BY event_type, event_at_rank_4_bucket), w_all_by_next_event_at_ties AS (ORDER BY next_event_at ASC)
)
-- Final projection
SELECT
    id,
    first_event_at,
    event_at_rank,
    event_at_rank_2,
    prev3_event_at,
    event_at_rank_3,
    event_at_rank_4_bucket,
    prev3_event_at_to_date_max_event_at_rank_2,
    second_event_at_rank_4,
    next_event_at_rank
FROM ranked
ORDER BY event_at_rank, event_at_rank_2 DESC
