SELECT
    CASE WHEN salary >= 152000 THEN 'high' WHEN salary >= 58000 THEN 'mid' ELSE 'low' END AS salary_bucket,
    level,
    MIN(salary) AS min_salary,
    MAX(hire_date) AS latest_hire_date
FROM employees
GROUP BY CASE WHEN salary >= 152000 THEN 'high' WHEN salary >= 58000 THEN 'mid' ELSE 'low' END, level
HAVING min_salary >= 114000
ORDER BY salary_bucket ASC;
