SELECT *
FROM departments
WHERE NOT (location NOT IN ('Chiang Mai', 'Berlin', 'Bangkok') OR dept_name IN ('Marketing', 'HR'))
