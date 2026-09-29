SELECT
    id,
    dept_name,
    budget,
    MIN(budget) OVER (PARTITION BY dept_name) AS dept_name_min_budget
FROM departments
WHERE budget > 2676000
ORDER BY dept_name_min_budget DESC, id DESC;
