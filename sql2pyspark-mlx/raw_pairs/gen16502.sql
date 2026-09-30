SELECT location, COUNT(*) AS cnt FROM departments WHERE dept_name IS NULL AND dept_name != 'Legal' GROUP BY location;
