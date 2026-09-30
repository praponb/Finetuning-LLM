SELECT
    CASE WHEN YEAR(hire_date) < 2024 THEN 'before_2024' WHEN YEAR(hire_date) = 2024 THEN '2024' ELSE 'after_2024' END AS hire_cohort,
    job_title,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    SUM(CASE WHEN job_title = 'Designer' THEN 1 ELSE 0 END) AS designer_count
FROM employees
WHERE level IN ('L2', 'L5') AND full_name IS NOT NULL AND dept_id IN (21, 40)
GROUP BY CASE WHEN YEAR(hire_date) < 2024 THEN 'before_2024' WHEN YEAR(hire_date) = 2024 THEN '2024' ELSE 'after_2024' END, job_title
ORDER BY designer_count DESC
