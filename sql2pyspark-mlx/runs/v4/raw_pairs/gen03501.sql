WITH valid AS (
    SELECT *, TRIM(dept_name) AS clean_dept_name
    FROM departments
    WHERE location <> 'London'
),
metrics AS (
    SELECT
        *,
        AVG(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS budget_avg_5,
        LAST_VALUE(location, TRUE) OVER w_ordered AS last_known_location,
        SUM(budget) OVER (PARTITION BY location) AS location_sum_budget,
        NTILE(3) OVER (PARTITION BY dept_name ORDER BY budget, id) AS budget_bucket,
        NTILE(3) OVER (PARTITION BY location ORDER BY budget DESC, id) AS budget_bucket_2
    FROM valid
    WINDOW w_ordered AS (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
matched_keys AS (
    SELECT id
    FROM metrics
    EXCEPT ALL
    SELECT id
    FROM departments
    WHERE budget > 1510000
),
verified AS (
    SELECT s.*
    FROM metrics s
    INNER JOIN matched_keys k ON s.id = k.id
),
not_flagged AS (
    SELECT s.*
    FROM verified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.job_title NOT IN ('Intern', 'Analyst', 'Manager')
        )
)
SELECT
    CASE WHEN budget_avg_5 >= 3957054 THEN 'LARGE' ELSE 'SMALL' END AS budget_avg_5_tier,
    CASE WHEN budget_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END AS budget_bucket_tier,
    location,
    COUNT_IF(dept_name = 'HR') AS hr_events,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(budget, ARRAY(0.25, 0.5, 0.75)) AS budget_quartiles,
    SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list
FROM not_flagged
GROUP BY CASE WHEN budget_avg_5 >= 3957054 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN budget_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END, location
HAVING cnt > 1;
