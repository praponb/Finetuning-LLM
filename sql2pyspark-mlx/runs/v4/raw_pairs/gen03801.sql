SELECT
    CASE WHEN salary >= 164000 THEN 'high' WHEN salary >= 82000 THEN 'mid' ELSE 'low' END salary_bucket,
    SUM(salary) total_salary,
    COUNT(*) cnt,
    SUM(SUM(salary)) OVER (ORDER BY CASE WHEN salary >= 164000 THEN 'high' WHEN salary >= 82000 THEN 'mid' ELSE 'low' END) AS cumulative_salary
FROM employees
GROUP BY CASE WHEN salary >= 164000 THEN 'high' WHEN salary >= 82000 THEN 'mid' ELSE 'low' END;
