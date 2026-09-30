SELECT
    job_title,
    SUM(salary) AS total_salary,
    COUNT(*) AS cnt,
    DENSE_RANK() OVER (ORDER BY SUM(salary) DESC) AS rnk
FROM employees
GROUP BY job_title
ORDER BY cnt
