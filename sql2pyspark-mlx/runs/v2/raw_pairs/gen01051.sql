WITH base AS (
    SELECT *
    FROM employees
    WHERE full_name LIKE '%Cha%' OR dept_id IN (23, 30, 34)
),
salary_bucket_summary AS (
    SELECT
        CASE WHEN salary >= 160000 THEN 'high' WHEN salary >= 75000 THEN 'mid' ELSE 'low' END salary_bucket,
        manager_id,
        MAX(salary) max_salary,
        SORT_ARRAY(COLLECT_SET(level)) level_list
    FROM base
    GROUP BY CASE WHEN salary >= 160000 THEN 'high' WHEN salary >= 75000 THEN 'mid' ELSE 'low' END, manager_id
)
SELECT
    salary_bucket,
    manager_id,
    max_salary,
    level_list
FROM salary_bucket_summary
WHERE max_salary < 121000
ORDER BY manager_id DESC
