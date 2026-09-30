SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS rn
    FROM departments
) AS r
WHERE rn <= 2
ORDER BY dept_name;
