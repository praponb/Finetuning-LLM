SELECT dept_name, avg_budget, max_budget
FROM (
    SELECT dept_name, AVG(budget) AS avg_budget, MAX(budget) AS max_budget
    FROM departments
    GROUP BY dept_name
) AS agg
WHERE avg_budget < 1780000;
