SELECT
    dept_name,
    SUM(budget) total_budget,
    COUNT(*) cnt,
    DENSE_RANK() OVER (ORDER BY SUM(budget) DESC) AS budget_rank
FROM departments
GROUP BY dept_name
ORDER BY 4 DESC;
