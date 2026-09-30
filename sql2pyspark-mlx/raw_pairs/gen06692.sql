SELECT region, SORT_ARRAY(COLLECT_SET(region)) AS region_list, ROUND((100 * SUM(CASE WHEN region = 'East' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_east FROM stores GROUP BY 1 ORDER BY pct_east
