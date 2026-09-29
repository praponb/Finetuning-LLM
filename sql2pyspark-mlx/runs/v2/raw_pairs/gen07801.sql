SELECT
    id,
    location,
    budget,
    dept_name,
    FIRST_VALUE(dept_name) OVER (PARTITION BY location ORDER BY budget, id) first_dept_name,
    MAX(budget) OVER (PARTITION BY location) location_max_budget
FROM departments
WHERE budget BETWEEN 1567000 AND 3774000
