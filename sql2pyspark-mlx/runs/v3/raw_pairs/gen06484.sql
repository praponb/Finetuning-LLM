SELECT location, dept_name, INITCAP(TRIM(dept_name)) dept_name_title
FROM departments
WHERE budget >= 2465000
ORDER BY location DESC;
