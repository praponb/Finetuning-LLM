WITH scoped_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        tags
    FROM tickets
    WHERE opened_at >= TIMESTAMP '2025-02-06 11:00:00'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_id ORDER BY opened_at ASC, id), 3) AS opened_at_percent_rank,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER w_priority_by_opened_at)) / 60, 1) AS minutes_since_prev_closed_at,
        NTILE(5) OVER w_priority_by_opened_at AS opened_at_quintile,
        FIRST_VALUE(closed_at) OVER (PARTITION BY priority ORDER BY opened_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_closed_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_id ORDER BY opened_at ASC), 3) AS opened_at_percent_rank_2,
        ROW_NUMBER() OVER w_priority_agent_id_by_closed AS rn,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER w_priority_by_opened_at)) / 60, 1) AS minutes_since_prev_opened_at,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY priority, agent_id ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_id, priority ORDER BY opened_at NULLS LAST, id), 3) AS opened_at_cume_dist,
        RANK() OVER w_priority_agent_id_by_closed AS closed_at_rank
    FROM scoped_tickets
    WINDOW w_priority_by_opened_at AS (PARTITION BY priority ORDER BY opened_at ASC, id), w_priority_agent_id_by_closed AS (PARTITION BY priority, agent_id ORDER BY closed_at DESC, id)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(minutes_since_prev_closed_at - AVG(minutes_since_prev_closed_at) OVER (PARTITION BY TO_DATE(first_closed_at), YEAR(closed_at)), 2) AS minutes_since_prev_closed_at_vs_first_closed_at_,
        DENSE_RANK() OVER (PARTITION BY opened_at_quintile, rn ORDER BY first_closed_at ASC) AS first_closed_at_dense_rank,
        FIRST_VALUE(first_closed_at) IGNORE NULLS OVER (PARTITION BY closed_at_rank, MONTH(second_closed_at) ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_first_closed_at,
        ROUND(AVG(opened_at_percent_rank) OVER (PARTITION BY opened_at_quintile ORDER BY first_closed_at ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS opened_at_percent_rank_avg_3_prior,
        ROUND(AVG(opened_at_percent_rank) OVER (PARTITION BY agent_id, closed_at_rank ORDER BY opened_at_cume_dist DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS opened_at_percent_rank_avg_4,
        ROUND(minutes_since_prev_closed_at - AVG(minutes_since_prev_closed_at) OVER (PARTITION BY TO_DATE(second_closed_at)), 2) AS minutes_since_prev_closed_at_vs_second_closed_at,
        NTH_VALUE(minutes_since_prev_opened_at, 2) OVER (PARTITION BY closed_at_rank, MONTH(second_closed_at) ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_minutes_since_prev_opened_at,
        COUNT(*) OVER (PARTITION BY opened_at_quintile, rn ORDER BY first_closed_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM metrics
)
SELECT
    id,
    agent_id,
    opened_at,
    opened_at_percent_rank,
    minutes_since_prev_closed_at,
    opened_at_quintile,
    first_closed_at,
    opened_at_percent_rank_2,
    rn,
    minutes_since_prev_opened_at,
    closed_at_rank,
    minutes_since_prev_closed_at_vs_first_closed_at_,
    first_known_first_closed_at,
    opened_at_percent_rank_avg_3_prior,
    opened_at_percent_rank_avg_4,
    minutes_since_prev_closed_at_vs_second_closed_at,
    second_minutes_since_prev_opened_at,
    opened_at_percent_rank_2 - rn AS opened_at_percent_rank_2_minus_rn
FROM with_trends
WHERE running_count >= 4
ORDER BY opened_at_percent_rank_avg_3_prior, opened_at DESC NULLS FIRST
