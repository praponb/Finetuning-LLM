SELECT dept_name, max_budget
FROM (
    SELECT dept_name, MAX(budget) AS max_budget
    FROM departments
    WHERE dept_name = 'Finance'
    GROUP BY dept_name
) AS agg
WHERE max_budget >= 2277000
ORDER BY dept_name DESC, max_budget DESC
LIMIT 1;
