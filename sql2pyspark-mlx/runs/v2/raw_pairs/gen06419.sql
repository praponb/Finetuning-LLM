WITH Src AS (
    SELECT *
    FROM departments
    WHERE budget BETWEEN 3523000 AND 3552000 AND dept_name IN ('Marketing', 'HR')
),
Agg AS (
    SELECT CASE WHEN budget >= 4190000 THEN 'high' WHEN budget >= 990000 THEN 'mid' ELSE 'low' END AS budget_bucket, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(location))) AS location_values
    FROM Src
    GROUP BY CASE WHEN budget >= 4190000 THEN 'high' WHEN budget >= 990000 THEN 'mid' ELSE 'low' END
)
SELECT budget_bucket, location_values
FROM Agg
