SELECT
    id,
    dept_name,
    budget,
    ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS rn
FROM departments
