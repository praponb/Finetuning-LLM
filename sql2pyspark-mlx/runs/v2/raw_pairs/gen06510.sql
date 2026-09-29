SELECT city, region, SORT_ARRAY(COLLECT_SET(region)) AS region_list
FROM stores
WHERE sqft <> 4590 AND (city IN ('Berlin', 'Bangkok', 'New York') OR store_name LIKE '%Mal%')
GROUP BY 1, 2
HAVING COUNT(*) >= 5
ORDER BY region ASC;
