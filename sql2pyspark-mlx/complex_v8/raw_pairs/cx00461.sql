WITH cleaned_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('created', 'assigned') THEN 'group_a' WHEN event_type = 'escalated' THEN 'group_b' ELSE 'other' END AS event_type_group,
        LOWER(TRIM(event_type)) AS event_type_clean
    FROM ticket_events
    WHERE event_type = 'escalated'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        event_type_clean,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at, id) AS rn,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at) AS event_at_rank,
        NTILE(3) OVER w_event_type_by_event_at AS event_at_tercile,
        FIRST_VALUE(event_at) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at,
        LEAD(event_at) OVER (PARTITION BY event_type ORDER BY event_at DESC, id) AS next_event_at,
        LAST_VALUE(event_at) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at) ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type_group ORDER BY event_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        LAG(event_at) OVER (PARTITION BY event_type_group ORDER BY event_at ASC NULLS LAST, id) AS prev_event_at,
        DENSE_RANK() OVER w_event_at_month_by_event_at_2 AS event_at_dense_rank,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_2
    FROM cleaned_events
    WINDOW w_event_type_by_event_at AS (PARTITION BY event_type ORDER BY event_at DESC, id), w_event_at_month_by_event_at_2 AS (PARTITION BY MONTH(event_at) ORDER BY event_at NULLS LAST, id)
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM sequenced
    WHERE rn = 1
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(first_known_event_at_2, 3) OVER w_event_at_tercile_by_first_ev AS next3_first_known_event_at_2,
        SUM(event_at_tercile) OVER (PARTITION BY event_at_tercile ORDER BY first_event_at DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS event_at_tercile_sum_2,
        SIZE(COLLECT_SET(event_type) OVER w_event_at_dense_rank) AS distinct_event_type_in_event_at_dense_rank,
        SUM(rn) OVER (PARTITION BY event_at_rank, DATE_TRUNC('MONTH', event_at)) AS event_at_rank_event_at_month_sum_rn,
        MAX(event_at_rank) OVER (PARTITION BY event_at_tercile ORDER BY first_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_event_at_rank_so_far
    FROM first_rows
    WINDOW w_event_at_tercile_by_first_ev AS (PARTITION BY event_at_tercile ORDER BY first_event_at DESC, id), w_event_at_dense_rank AS (PARTITION BY event_at_dense_rank)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type_clean,
    rn,
    event_at_rank,
    first_event_at,
    minutes_since_prev_event_at,
    next_event_at,
    last_event_at,
    prev_event_at,
    event_at_dense_rank,
    first_known_event_at_2,
    next3_first_known_event_at_2,
    event_at_tercile_sum_2,
    max_event_at_rank_so_far
FROM with_trends
ORDER BY event_at_tercile_sum_2 DESC, minutes_since_prev_event_at, id
LIMIT 5
