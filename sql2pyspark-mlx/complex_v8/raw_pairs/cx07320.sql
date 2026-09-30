WITH recent_agents AS (
    -- Scope the support_agents rows
    SELECT *
    FROM support_agents
    WHERE hired_on IS NOT NULL
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        DATEDIFF(hired_on, LAG(hired_on) OVER (ORDER BY hired_on DESC, id)) AS days_since_prev_hired_on,
        ROUND(PERCENT_RANK() OVER (PARTITION BY tier, YEAR(hired_on) ORDER BY hired_on, id), 3) AS hired_on_percent_rank,
        ROUND(PERCENT_RANK() OVER w_all_by_hired_on, 3) AS hired_on_percent_rank_2,
        RANK() OVER (PARTITION BY tier ORDER BY hired_on ASC NULLS LAST, id) AS hired_on_rank,
        LAST_VALUE(hired_on) IGNORE NULLS OVER (PARTITION BY tier, YEAR(hired_on) ORDER BY hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_hired_on,
        DENSE_RANK() OVER (PARTITION BY tier, YEAR(hired_on) ORDER BY hired_on) AS hired_on_dense_rank,
        LAST_VALUE(hired_on) IGNORE NULLS OVER (PARTITION BY tier ORDER BY hired_on ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_hired_on_2,
        NTH_VALUE(hired_on, 2) OVER (PARTITION BY tier ORDER BY hired_on ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_hired_on,
        ROUND(CUME_DIST() OVER (ORDER BY hired_on DESC, id), 3) AS hired_on_cume_dist,
        DATEDIFF(hired_on, LAG(hired_on) OVER (PARTITION BY tier, YEAR(hired_on) ORDER BY hired_on, id)) AS days_since_prev_hired_on_2,
        RANK() OVER w_all_by_hired_on AS hired_on_rank_2,
        LAG(hired_on, 2) OVER w_tier_by_hired_on AS prev2_hired_on,
        LAST_VALUE(hired_on) IGNORE NULLS OVER (ORDER BY hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_hired_on_3,
        NTH_VALUE(hired_on, 2) OVER (ORDER BY hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_hired_on_2,
        FIRST_VALUE(hired_on) OVER (PARTITION BY tier, YEAR(hired_on) ORDER BY hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_hired_on
    FROM recent_agents
    WINDOW w_all_by_hired_on AS (ORDER BY hired_on DESC, id), w_tier_by_hired_on AS (PARTITION BY tier ORDER BY hired_on ASC NULLS LAST, id)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(hired_on_rank) OVER (PARTITION BY hired_on_rank, team ORDER BY hired_on_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_hired_on_rank_so_far,
        MAX(days_since_prev_hired_on) OVER (ORDER BY hired_on_cume_dist, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS days_since_prev_hired_on_max_7,
        MAX(hired_on_rank_2) OVER (PARTITION BY hired_on_rank ORDER BY second_hired_on_2 DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS hired_on_rank_2_max_2,
        NTILE(3) OVER (PARTITION BY hired_on_rank, team ORDER BY hired_on_dense_rank, id) AS hired_on_dense_rank_tercile
    FROM windowed
)
-- Final projection
SELECT
    id,
    agent_name,
    tier,
    hired_on_percent_rank,
    hired_on_rank,
    last_known_hired_on,
    hired_on_dense_rank,
    last_known_hired_on_2,
    second_hired_on,
    hired_on_cume_dist,
    days_since_prev_hired_on_2,
    hired_on_rank_2,
    prev2_hired_on,
    second_hired_on_2,
    first_hired_on,
    max_hired_on_rank_so_far,
    hired_on_rank_2_max_2,
    hired_on_dense_rank_tercile,
    DATE_TRUNC('MONTH', last_known_hired_on_3) AS last_known_hired_on_3_month
FROM with_trends
ORDER BY hired_on_dense_rank DESC, hired_on_rank_2_max_2 NULLS LAST
