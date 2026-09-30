SELECT dept_name, num_departments, singapore_budget
FROM (
    SELECT dept_name, COUNT(*) AS num_departments, SUM(CASE WHEN location = 'Singapore' THEN budget ELSE 0 END) AS singapore_budget
    FROM departments
    WHERE budget >= 2077000
    GROUP BY dept_name
) AS s
WHERE num_departments < 7;
