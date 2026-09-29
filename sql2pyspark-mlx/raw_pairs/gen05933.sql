WITH base AS (
    -- Keep recent, valid rows
    SELECT *, UPPER(dept_name) AS dept_name_upper
    FROM departments
    WHERE dept_name IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        STDDEV_SAMP(budget) OVER (PARTITION BY location) AS location_stddev_samp_budget,
        CUME_DIST() OVER (PARTITION BY location, dept_name ORDER BY budget DESC) AS budget_cume_dist,
        MAX(budget) OVER (PARTITION BY dept_name) AS dept_name_max_budget,
        AVG(budget) OVER (ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS budget_avg_2_prior,
        SUM(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget
    FROM base
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(location, 'Unknown') AS location_filled
    FROM with_windows
    WHERE dept_name IS NOT NULL
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(budget, 0) AS budget_filled
    FROM cleaned
    WHERE location IS NOT NULL
)
-- Multi-level aggregation
SELECT
    CASE WHEN running_budget >= 21274066 THEN 'HIGH' ELSE 'LOW' END AS running_budget_tier,
    CASE WHEN budget_avg_2_prior >= 3757273 THEN 'HIGH' ELSE 'LOW' END AS budget_avg_2_prior_tier,
    CASE WHEN location_stddev_samp_budget >= 3557793 THEN 'PREMIUM' ELSE 'STANDARD' END AS location_stddev_samp_budget_tier,
    COUNT(CASE WHEN dept_name = 'HR' THEN 1 END) AS hr_count,
    APPROX_PERCENTILE(budget_filled, ARRAY(0.25, 0.5, 0.75)) AS budget_filled_quartiles,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN running_budget >= 21274066 THEN 'HIGH' ELSE 'LOW' END ORDER BY COUNT(CASE WHEN dept_name = 'HR' THEN 1 END) DESC) AS hr_count_dense_rank
FROM valid
GROUP BY running_budget_tier, budget_avg_2_prior_tier, location_stddev_samp_budget_tier
ORDER BY location_stddev_samp_budget_tier DESC NULLS FIRST;
