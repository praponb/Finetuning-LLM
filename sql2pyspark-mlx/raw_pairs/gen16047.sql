WITH filtered_departments AS (
    SELECT *
    FROM departments
    WHERE budget <= 2030000 OR budget < 1371000
),
dept_name_summary AS (
    SELECT dept_name, MAX(budget) AS max_budget, SORT_ARRAY(COLLECT_SET(location)) AS location_list
    FROM filtered_departments
    GROUP BY dept_name
)
SELECT *
FROM dept_name_summary
WHERE max_budget < 2558000
ORDER BY max_budget DESC;
