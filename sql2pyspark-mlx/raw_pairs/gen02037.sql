SELECT
    id,
    dept_name,
    location,
    budget
FROM departments
QUALIFY ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) = 1;
