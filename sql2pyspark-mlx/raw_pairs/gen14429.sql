SELECT
    id,
    dept_name,
    budget,
    location,
    RANK() OVER (PARTITION BY dept_name ORDER BY budget) AS budget_rank,
    RANK() OVER (PARTITION BY location ORDER BY budget) AS budget_rank_2
FROM departments
WHERE dept_name NOT IN ('Legal', 'Sales');
