SELECT
    id,
    dept_name,
    budget,
    INITCAP(TRIM(dept_name)) AS dept_name_title
FROM departments
WHERE location = 'Bangkok' AND budget <= 1576000 AND budget IS NOT NULL;
