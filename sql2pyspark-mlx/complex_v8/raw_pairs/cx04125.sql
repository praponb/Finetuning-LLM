WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        hired_on,
        DAYOFWEEK(hired_on) AS hired_on_dayofweek
    FROM support_agents
    WHERE hired_on >= DATE '2024-05-02'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(hired_on_dayofweek - AVG(hired_on_dayofweek) OVER (PARTITION BY hired_on_dayofweek), 2) AS hired_on_dayofweek_vs_hired_on_dayofweek_avg,
        LAG(hired_on_dayofweek) OVER (PARTITION BY MONTH(hired_on), hired_on_dayofweek ORDER BY hired_on ASC, id) AS prev_hired_on_dayofweek,
        NTILE(3) OVER (ORDER BY hired_on_dayofweek NULLS LAST, id) AS hired_on_dayofweek_tercile,
        MAX(hired_on_dayofweek) OVER (PARTITION BY MONTH(hired_on), hired_on_dayofweek ORDER BY hired_on ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS hired_on_dayofweek_max_1_prior,
        DENSE_RANK() OVER (ORDER BY hired_on_dayofweek NULLS LAST) AS hired_on_dayofweek_dense_rank,
        SUM(hired_on_dayofweek) OVER (ORDER BY hired_on_dayofweek NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS hired_on_dayofweek_sum_2_prior,
        COUNT(*) OVER (PARTITION BY hired_on_dayofweek) AS rows_in_hired_on_dayofweek,
        LAG(hired_on) OVER (PARTITION BY MONTH(hired_on), team ORDER BY hired_on_dayofweek, id) AS prev_hired_on,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(hired_on), team ORDER BY hired_on_dayofweek, id), 3) AS hired_on_dayofweek_cume_dist
    FROM valid
),
leaders AS (
    -- Top 2 per group
    SELECT
        id,
        agent_name,
        team,
        hired_on,
        hired_on_dayofweek,
        hired_on_dayofweek_vs_hired_on_dayofweek_avg,
        prev_hired_on_dayofweek,
        hired_on_dayofweek_tercile,
        hired_on_dayofweek_max_1_prior,
        hired_on_dayofweek_sum_2_prior,
        rows_in_hired_on_dayofweek,
        prev_hired_on,
        hired_on_dayofweek_cume_dist
    FROM with_windows
    WHERE hired_on_dayofweek_dense_rank <= 2
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(hired_on) IGNORE NULLS OVER (ORDER BY hired_on_dayofweek_sum_2_prior ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_hired_on,
        hired_on_dayofweek_cume_dist - LAG(hired_on_dayofweek_cume_dist) OVER (ORDER BY hired_on_dayofweek_sum_2_prior ASC, id) AS hired_on_dayofweek_cume_dist_change,
        MAX(hired_on_dayofweek) OVER (PARTITION BY hired_on_dayofweek ORDER BY prev_hired_on_dayofweek, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_hired_on_dayofweek_so_far
    FROM leaders
)
-- Final projection
SELECT
    id,
    agent_name,
    hired_on_dayofweek_vs_hired_on_dayofweek_avg,
    prev_hired_on_dayofweek,
    hired_on_dayofweek_max_1_prior,
    rows_in_hired_on_dayofweek,
    prev_hired_on,
    hired_on_dayofweek_cume_dist,
    last_known_hired_on,
    hired_on_dayofweek_cume_dist_change,
    max_hired_on_dayofweek_so_far,
    prev_hired_on_dayofweek - hired_on_dayofweek AS prev_hired_on_dayofweek_minus_hired_on_dayofweek,
    DATEDIFF(prev_hired_on, last_known_hired_on) AS days_last_known_hired_on_to_prev_hired_on
FROM sequenced
