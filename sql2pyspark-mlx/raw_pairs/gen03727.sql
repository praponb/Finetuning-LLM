SELECT *
FROM (
    SELECT dept_name, COUNT(budget) budget_count
    FROM departments
    GROUP BY dept_name
) x
WHERE budget_count < 7
ORDER BY 2 DESC
