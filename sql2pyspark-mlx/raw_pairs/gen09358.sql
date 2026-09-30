SELECT *
FROM (
    SELECT
        DATE_FORMAT(hire_date, 'yyyy-MM') AS hire_month,
        is_remote,
        AVG(salary) AS avg_salary,
        MAX(salary) AS max_salary
    FROM employees
    WHERE salary >= 95000
    GROUP BY DATE_FORMAT(hire_date, 'yyyy-MM'), is_remote
) AS t
WHERE avg_salary >= 94000;
