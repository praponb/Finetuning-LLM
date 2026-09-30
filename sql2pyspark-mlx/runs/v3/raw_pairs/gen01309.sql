SELECT
    id,
    dept_name,
    budget,
    INITCAP(TRIM(dept_name)) AS dept_name_title
FROM departments;
