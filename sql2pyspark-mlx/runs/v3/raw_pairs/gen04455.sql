SELECT
    dept_name,
    SUM(budget) AS total_budget,
    COUNT(*) AS cnt,
    DENSE_RANK() OVER (ORDER BY SUM(budget) DESC) AS budget_rank
FROM departments
WHERE budget <= 3523000
GROUP BY dept_name
ORDER BY total_budget DESC;
