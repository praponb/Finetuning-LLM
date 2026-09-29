WITH kept_keys AS (
    SELECT dept_name
    FROM departments
    EXCEPT ALL
    SELECT dept_name
    FROM departments
    WHERE location IN ('Bangkok', 'London')
),
verified AS (
    SELECT s.*
    FROM departments s
    JOIN kept_keys k ON s.dept_name = k.dept_name
),
kept_keys_2 AS (
    SELECT dept_name, id
    FROM verified
    EXCEPT
    SELECT dept_name, id
    FROM departments
    WHERE dept_name = 'Marketing'
),
kept_rows AS (
    SELECT s.*
    FROM verified s
    JOIN kept_keys_2 k ON s.dept_name = k.dept_name AND s.id = k.id
),
with_windows AS (
    SELECT
        dept_name,
        location,
        budget,
        CUME_DIST() OVER (ORDER BY budget DESC NULLS FIRST) AS budget_cume_dist
    FROM kept_rows
),
metrics AS (
    SELECT
        dept_name,
        location,
        budget_cume_dist,
        CUME_DIST() OVER (ORDER BY budget_cume_dist) AS budget_cume_dist_cume_dist,
        CUME_DIST() OVER (PARTITION BY location ORDER BY budget DESC) AS budget_cume_dist_2,
        RANK() OVER (ORDER BY budget) AS budget_rank,
        CUME_DIST() OVER (PARTITION BY location ORDER BY budget_cume_dist NULLS FIRST) AS budget_cume_dist_cume_dist_2,
        CUME_DIST() OVER (PARTITION BY dept_name ORDER BY budget DESC NULLS FIRST) AS budget_cume_dist_3
    FROM with_windows
),
metrics_departments AS (
    SELECT *, PERCENT_RANK() OVER (ORDER BY budget_cume_dist_3 NULLS FIRST) AS budget_cume_dist_3_percent_rank
    FROM metrics
)
SELECT
    dept_name,
    location,
    budget_cume_dist_cume_dist,
    budget_cume_dist_2,
    budget_cume_dist_cume_dist_2,
    budget_cume_dist_3_percent_rank
FROM metrics_departments
