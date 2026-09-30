SELECT region, MAX(sqft) - MIN(sqft) AS sqft_range
FROM stores
WHERE NOT (sqft <> 12724 AND LOWER(TRIM(store_name)) = 'mall central')
GROUP BY region
ORDER BY region ASC;
