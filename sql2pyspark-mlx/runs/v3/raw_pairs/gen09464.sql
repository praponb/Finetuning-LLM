SELECT
    CASE WHEN salary >= 146000 THEN 'high' WHEN salary >= 67000 THEN 'mid' ELSE 'low' END salary_bucket,
    SUM(salary) total_salary,
    COUNT(*) cnt,
    DENSE_RANK() OVER (ORDER BY SUM(salary) DESC) AS rnk
FROM employees
GROUP BY CASE WHEN salary >= 146000 THEN 'high' WHEN salary >= 67000 THEN 'mid' ELSE 'low' END;
