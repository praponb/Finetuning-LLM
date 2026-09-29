SELECT hire_month, min_salary
FROM (
    SELECT DATE_TRUNC('MONTH', hire_date) AS hire_month, MIN(salary) AS min_salary
    FROM employees
    GROUP BY DATE_TRUNC('MONTH', hire_date)
) AS agg
WHERE min_salary >= 110000
