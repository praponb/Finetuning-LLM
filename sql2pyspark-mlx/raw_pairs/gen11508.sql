SELECT
    dept_name,
    location,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(dept_name))) AS dept_name_values,
    MAX(budget) AS max_budget
FROM departments
WHERE budget <= 3847000
GROUP BY dept_name, location;
