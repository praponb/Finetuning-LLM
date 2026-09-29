SELECT *, LOWER(location) location_lower FROM departments WHERE dept_name NOT IN ('Marketing', 'Legal') AND location = 'Chiang Mai' ORDER BY location;
