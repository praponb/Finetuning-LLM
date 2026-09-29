SELECT
    DATE_TRUNC('MONTH', hire_date) AS hire_month,
    ROUND(AVG(salary), 2) AS avg_salary,
    SORT_ARRAY(COLLECT_SET(level)) AS level_list,
    COUNT(DISTINCT salary) AS unique_job_titles
FROM employees
GROUP BY DATE_TRUNC('MONTH', hire_date)
ORDER BY avg_salary;
