WITH enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget DESC, id) AS seq_no_2,
        MAX(budget) OVER (PARTITION BY location) AS location_max_budget,
        AVG(budget) OVER (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS budget_avg_3,
        LAST_VALUE(dept_name, TRUE) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_dept_name
    FROM departments
)
SELECT
    seq_no,
    CASE WHEN location_max_budget >= 3393791 THEN 'HIGH' ELSE 'LOW' END AS location_max_budget_tier,
    STDDEV_SAMP(budget) AS budget_stddev,
    SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list,
    SUM(CASE WHEN dept_name = 'Finance' THEN budget_avg_3 ELSE 0 END) AS finance_budget_avg_3,
    COUNT(*) AS cnt
FROM enriched
GROUP BY seq_no, CASE WHEN location_max_budget >= 3393791 THEN 'HIGH' ELSE 'LOW' END
HAVING finance_budget_avg_3 > 0
