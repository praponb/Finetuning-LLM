SELECT id, location, LOWER(dept_name) AS dept_name_lower FROM departments WHERE budget IS NOT NULL AND (budget IS NOT NULL OR budget <= 3695000)
