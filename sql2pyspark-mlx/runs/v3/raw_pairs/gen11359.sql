SELECT location, SUBSTRING(dept_name, 1, 4) AS dept_name_prefix, INITCAP(TRIM(location)) AS location_title FROM departments WHERE location IN ('Berlin', 'Singapore') ORDER BY location_title;
