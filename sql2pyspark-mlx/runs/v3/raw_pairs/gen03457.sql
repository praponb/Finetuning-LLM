SELECT
    location,
    dept_name,
    dept_name_values,
    legal_count
FROM (
    SELECT
        location,
        dept_name,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(dept_name))) AS dept_name_values,
        COUNT(CASE WHEN dept_name = 'Legal' THEN 1 END) AS legal_count
    FROM departments
    GROUP BY location, dept_name
) AS t
WHERE legal_count > 5;
