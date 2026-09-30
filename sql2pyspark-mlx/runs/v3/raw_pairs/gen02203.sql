SELECT dept_name, cnt
FROM (
    SELECT dept_name, COUNT(*) AS cnt
    FROM departments
    GROUP BY dept_name
) AS s
WHERE cnt > 4
ORDER BY cnt DESC
