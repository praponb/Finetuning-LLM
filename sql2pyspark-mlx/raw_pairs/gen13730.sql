SELECT
    id,
    dept_name,
    location,
    budget,
    AVG(budget) OVER w_recent AS budget_avg_5,
    ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name, location), 4) AS budget_share
FROM departments
WINDOW w_recent AS (PARTITION BY dept_name, location ORDER BY budget ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
