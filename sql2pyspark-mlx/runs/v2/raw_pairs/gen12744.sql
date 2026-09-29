SELECT
    id,
    budget,
    INITCAP(TRIM(location)) AS location_title,
    CONCAT_WS(',', location, dept_name) AS location_dept_name_key
FROM departments
WHERE dept_name = 'Legal' AND location <> 'Berlin'
