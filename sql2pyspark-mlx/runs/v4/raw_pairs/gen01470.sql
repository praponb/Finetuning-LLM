SELECT
    id,
    location,
    dept_name,
    COALESCE(location, 'unassigned') location_filled,
    LOWER(dept_name) dept_name_lower
FROM departments
WHERE location = 'Chiang Mai' AND dept_name = 'Marketing' AND budget BETWEEN 1084000 AND 2088000;
