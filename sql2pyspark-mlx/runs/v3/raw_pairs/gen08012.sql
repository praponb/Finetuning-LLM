WITH Base AS (
    SELECT *, SUBSTRING(full_name, 1, 2) AS full_name_prefix
    FROM employees
    WHERE full_name NOT LIKE '%Oli%' AND manager_id = 34
),
agg AS (
    SELECT CASE WHEN YEAR(hire_date) < 2024 THEN 'before_2024' WHEN YEAR(hire_date) = 2024 THEN '2024' ELSE 'after_2024' END AS hire_cohort, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) AS job_title_values
    FROM Base
    GROUP BY CASE WHEN YEAR(hire_date) < 2024 THEN 'before_2024' WHEN YEAR(hire_date) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT hire_cohort, job_title_values
FROM agg
ORDER BY hire_cohort
LIMIT 50;
