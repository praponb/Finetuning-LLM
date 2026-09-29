SELECT
    location,
    dept_name,
    budget,
    LOWER(location) location_lower,
    CAST(budget AS INT) budget_int
FROM departments
WHERE dept_name IN ('Sales', 'Finance', 'Engineering') AND budget BETWEEN 1664000 AND 3819000;
