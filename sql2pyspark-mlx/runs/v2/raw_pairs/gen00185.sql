SELECT
    id,
    dept_name,
    budget,
    ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS row_num
FROM departments
WHERE location NOT IN ('Berlin', 'Tokyo');
