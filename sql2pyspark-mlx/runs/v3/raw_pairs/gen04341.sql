SELECT id, dept_name, budget
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY dept_name ORDER BY budget) AS rn
    FROM departments
    WHERE budget >= 2417000
) AS ranked
WHERE rn <= 1
