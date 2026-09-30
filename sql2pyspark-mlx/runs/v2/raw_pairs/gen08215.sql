SELECT *
FROM (
    SELECT CASE WHEN budget >= 4458000 THEN 'high' WHEN budget >= 1002000 THEN 'mid' ELSE 'low' END AS budget_bucket, COUNT(*) AS total_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(location))) AS location_values
    FROM departments
    WHERE budget > 1972000
    GROUP BY CASE WHEN budget >= 4458000 THEN 'high' WHEN budget >= 1002000 THEN 'mid' ELSE 'low' END
) AS sub
WHERE total_count >= 4;
