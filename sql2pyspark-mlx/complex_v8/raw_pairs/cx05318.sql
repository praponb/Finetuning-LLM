WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        tags
    FROM tickets
    WHERE HOUR(opened_at) BETWEEN 9 AND 11
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER (ORDER BY closed_at, id))) / 60, 1) AS minutes_since_prev_closed_at,
        ROUND(CUME_DIST() OVER (ORDER BY opened_at DESC), 3) AS opened_at_cume_dist,
        ROUND(CUME_DIST() OVER (PARTITION BY priority, MONTH(closed_at) ORDER BY opened_at DESC NULLS FIRST), 3) AS opened_at_cume_dist_2,
        RANK() OVER w_all_by_closed_at AS closed_at_rank,
        LEAD(opened_at, 2) OVER (ORDER BY opened_at DESC, id) AS next2_opened_at,
        ROW_NUMBER() OVER w_all_by_closed_at AS closed_at_seq,
        LAG(closed_at, 3) OVER (ORDER BY closed_at, id) AS prev3_closed_at,
        LAST_VALUE(opened_at) IGNORE NULLS OVER (ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_at,
        ROUND(PERCENT_RANK() OVER w_all_by_closed_at_ties, 3) AS closed_at_percent_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY closed_at, id), 3) AS closed_at_percent_rank_2,
        LEAD(opened_at) OVER (ORDER BY closed_at, id) AS next_opened_at,
        DENSE_RANK() OVER w_priority_closed_at_month_by AS opened_at_dense_rank
    FROM cleaned
    WINDOW w_all_by_closed_at AS (ORDER BY closed_at, id), w_all_by_closed_at_ties AS (ORDER BY closed_at), w_priority_closed_at_month_by AS (PARTITION BY priority, MONTH(closed_at) ORDER BY opened_at DESC NULLS FIRST)
),
top_per_group AS (
    -- Top 3 per group
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        tags,
        minutes_since_prev_closed_at,
        opened_at_cume_dist,
        opened_at_cume_dist_2,
        next2_opened_at,
        closed_at_seq,
        prev3_closed_at,
        last_known_opened_at,
        closed_at_percent_rank,
        closed_at_percent_rank_2,
        next_opened_at,
        opened_at_dense_rank
    FROM with_trends
    WHERE closed_at_rank <= 3
      AND closed_at_seq <= 7
)
-- Final projection
SELECT
    id,
    opened_at,
    closed_at,
    tags,
    minutes_since_prev_closed_at,
    opened_at_cume_dist,
    next2_opened_at,
    closed_at_seq,
    prev3_closed_at,
    last_known_opened_at,
    closed_at_percent_rank,
    closed_at_percent_rank_2,
    opened_at_dense_rank,
    UPPER(TRIM(priority)) AS priority_clean,
    DATEDIFF(DATE '2025-12-31', last_known_opened_at) AS days_since_last_known_opened_at
FROM top_per_group
ORDER BY closed_at DESC, id
LIMIT 5
