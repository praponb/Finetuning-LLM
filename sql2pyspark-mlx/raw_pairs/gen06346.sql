SELECT
    id,
    dept_name,
    budget,
    AVG(budget) OVER (PARTITION BY dept_name) AS dept_name_avg_budget,
    NTILE(2) OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS bucket
FROM departments;
