SELECT *
FROM (
    SELECT
        dept_name,
        CASE WHEN budget >= 3977000 THEN 'high' WHEN budget >= 2052000 THEN 'mid' ELSE 'low' END AS budget_bucket,
        SUM(CASE WHEN location = 'Chiang Mai' THEN budget ELSE 0 END) AS chiang_mai_budget,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(location))) AS location_values
    FROM departments
    WHERE location = 'Chiang Mai'
    GROUP BY dept_name, CASE WHEN budget >= 3977000 THEN 'high' WHEN budget >= 2052000 THEN 'mid' ELSE 'low' END
) AS t
WHERE chiang_mai_budget > 62
ORDER BY chiang_mai_budget, dept_name;
