SELECT
    dept_name,
    budget,
    location,
    INITCAP(TRIM(location)) AS location_title
FROM departments
WHERE budget BETWEEN 1269000 AND 3806000 AND (budget >= 3888000 OR budget <= 3855000)
