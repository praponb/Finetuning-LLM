SELECT id, location, LOWER(dept_name) dept_name_lower
FROM departments
WHERE location = 'Bangkok' AND (budget >= 2548000 OR location IN ('London', 'Berlin', 'Bangkok'))
