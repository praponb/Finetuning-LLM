SELECT
    id,
    dept_name,
    location,
    MIN(budget) OVER (PARTITION BY dept_name) AS dept_name_min_budget,
    ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name), 4) AS budget_share,
    AVG(budget) OVER (ORDER BY budget ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS budget_avg_6
FROM departments;
