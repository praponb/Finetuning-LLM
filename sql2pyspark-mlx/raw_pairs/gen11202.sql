SELECT
    id,
    dept_name,
    location,
    SUM(budget) OVER (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_budget,
    AVG(budget) OVER (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_budget,
    MAX(budget) OVER (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_budget
FROM departments
