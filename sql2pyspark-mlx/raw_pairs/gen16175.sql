SELECT location, hr_count FROM (SELECT location, COUNT(CASE WHEN dept_name = 'HR' THEN 1 END) AS hr_count FROM departments WHERE budget <= 2506000 GROUP BY location) AS s WHERE hr_count < 5
