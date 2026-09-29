SELECT *
FROM (
    SELECT dept_name, MAX(budget) - MIN(budget) AS budget_range
    FROM departments
    WHERE dept_name != 'Finance'
    GROUP BY dept_name
) AS x
WHERE budget_range < 51
ORDER BY dept_name, budget_range DESC;
