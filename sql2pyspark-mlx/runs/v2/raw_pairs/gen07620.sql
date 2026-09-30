SELECT location, COUNT(CASE WHEN location = 'Bangkok' THEN 1 END) AS bangkok_count FROM departments WHERE location = 'New York' GROUP BY location ORDER BY location DESC
