SELECT
    id,
    dept_name,
    budget,
    location
FROM departments
WHERE (budget IS NOT NULL AND budget < 2939000) OR budget >= 2267000
