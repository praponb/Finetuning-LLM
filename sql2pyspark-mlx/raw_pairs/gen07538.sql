WITH cleaned AS (
    SELECT *, LENGTH(dept_name) AS dept_name_length
    FROM departments
    WHERE location IS NOT DISTINCT FROM 'Bangkok'
      AND budget BETWEEN 1345000 AND 1992000
)
SELECT
    CASE WHEN dept_name_length >= 84 THEN 'PREMIUM' ELSE 'STANDARD' END AS dept_name_length_tier,
    dept_name,
    COUNT_IF(location = 'Chiang Mai') AS chiang_mai_events,
    SIZE(COLLECT_SET(location)) AS num_locations,
    COUNT(*) AS num_rows,
    ROUND(AVG(budget), 2) AS avg_budget,
    SORT_ARRAY(COLLECT_SET(location)) AS location_list,
    SIZE(COLLECT_SET(location)) AS num_locations_2
FROM cleaned
GROUP BY ROLLUP (CASE WHEN dept_name_length >= 84 THEN 'PREMIUM' ELSE 'STANDARD' END, dept_name)
ORDER BY chiang_mai_events DESC NULLS LAST
