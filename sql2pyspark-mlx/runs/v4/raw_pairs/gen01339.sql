SELECT id, budget, location, UPPER(location) AS location_upper, CONCAT(location, ' - ', dept_name) AS location_dept_name FROM departments WHERE budget BETWEEN 2024000 AND 2668000 ORDER BY 5
