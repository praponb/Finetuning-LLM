SELECT
    id,
    budget,
    CONCAT_WS('/', dept_name, location) AS dept_name_location_key,
    COALESCE(budget, 0) AS budget_filled
FROM departments
WHERE location != 'Bangkok';
