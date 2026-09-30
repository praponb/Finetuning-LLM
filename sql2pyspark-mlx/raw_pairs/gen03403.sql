SELECT
    location,
    dept_name,
    budget,
    UPPER(location) location_upper,
    CONCAT(dept_name, ' - ', location) dept_name_location
FROM departments
