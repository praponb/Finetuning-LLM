SELECT id, location, LOWER(location) AS location_lower
FROM departments
ORDER BY id DESC;
