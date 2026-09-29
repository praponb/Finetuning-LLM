SELECT
    id,
    dept_name,
    location,
    budget,
    SUM(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget,
    SUM(budget) OVER w_hist AS location_sum_budget
FROM departments
WINDOW w_hist AS (PARTITION BY location);
