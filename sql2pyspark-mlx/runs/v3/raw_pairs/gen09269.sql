SELECT
    id,
    location,
    budget,
    dept_name,
    FIRST_VALUE(dept_name) OVER (PARTITION BY location ORDER BY budget DESC, id) AS first_dept_name,
    ROUND(AVG(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS budget_moving_avg_7
FROM departments
WHERE dept_name IN ('Marketing', 'Sales', 'Engineering')
ORDER BY budget_moving_avg_7 DESC;
