WITH filtered_employees AS (
    SELECT *
    FROM employees
    WHERE dept_id IN (9, 10, 19)
),
job_title_summary AS (
    SELECT job_title, DATE_FORMAT(hire_date, 'yyyy-MM') hire_month, SORT_ARRAY(COLLECT_SET(level)) level_list
    FROM filtered_employees
    GROUP BY job_title, DATE_FORMAT(hire_date, 'yyyy-MM')
)
SELECT *
FROM job_title_summary;
