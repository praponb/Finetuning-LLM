WITH cleaned AS (
    SELECT
        *,
        CASE WHEN budget >= 3211000 THEN 'High' WHEN budget >= 1624000 THEN 'Medium' ELSE 'Low' END AS budget_level,
        COALESCE(dept_name, 'DIRECT') AS dept_name_clean
    FROM departments
    WHERE dept_name IS NOT NULL
),
with_windows AS (
    SELECT
        *,
        AVG(budget) OVER w_running AS budget_avg_2_prior,
        DENSE_RANK() OVER w_running_2 AS budget_dense_rank,
        SUM(budget) OVER (PARTITION BY location, dept_name ORDER BY budget ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget
    FROM cleaned
    WHERE dept_name IN ('Sales', 'HR')
    WINDOW w_running AS (ORDER BY budget ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), w_running_2 AS (ORDER BY budget DESC NULLS LAST)
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget_dense_rank DESC, id ASC) AS row_num
        FROM with_windows
    ) x
    WHERE row_num = 1
),
cleaned_departments AS (
    SELECT
        *,
        ROUND(budget_avg_2_prior * 0.85, 2) AS adjusted_budget_avg_2_prior,
        COALESCE(location, 'OTHER') AS location_clean
    FROM latest
    WHERE dept_name IS NOT NULL
),
with_windows_departments AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY location ORDER BY adjusted_budget_avg_2_prior ASC NULLS FIRST) AS adjusted_budget_avg_2_prior_cume_dist,
        SUM(adjusted_budget_avg_2_prior) OVER (PARTITION BY location, dept_name ORDER BY row_num ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_adjusted_budget_avg_2_prior,
        ROUND(budget_dense_rank / SUM(budget_dense_rank) OVER (PARTITION BY location), 4) AS budget_dense_rank_share
    FROM cleaned_departments
),
metrics AS (
    SELECT
        *,
        ROUND(budget_avg_2_prior / SUM(budget_avg_2_prior) OVER (PARTITION BY dept_name), 4) AS budget_avg_2_prior_share,
        DENSE_RANK() OVER (PARTITION BY location ORDER BY adjusted_budget_avg_2_prior_cume_dist ASC NULLS LAST) AS adjusted_budget_avg_2_prior_cume_dist_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY adjusted_budget_avg_2_prior_cume_dist DESC, id ASC) AS event_seq,
        MIN(budget_dense_rank_share) OVER w_recent AS dept_name_min_budget_dense_rank_share
    FROM with_windows_departments
    WINDOW w_recent AS (PARTITION BY dept_name)
)
SELECT
    CASE WHEN cumulative_adjusted_budget_avg_2_prior >= 400 THEN 'HIGH' ELSE 'LOW' END AS cumulative_adjusted_budget_avg_2_prior_tier,
    location,
    SIZE(COLLECT_SET(location)) AS num_locations,
    SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list,
    SIZE(COLLECT_SET(dept_name)) AS num_dept_names,
    RANK() OVER (PARTITION BY CASE WHEN cumulative_adjusted_budget_avg_2_prior >= 400 THEN 'HIGH' ELSE 'LOW' END ORDER BY SIZE(COLLECT_SET(dept_name)) DESC) AS num_dept_names_rank
FROM metrics
GROUP BY cumulative_adjusted_budget_avg_2_prior_tier, location
HAVING SIZE(COLLECT_SET(location)) > 1
