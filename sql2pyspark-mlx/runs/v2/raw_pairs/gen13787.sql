SELECT store_id, MIN(revenue) min_revenue, MAX(units) - MIN(units) units_range
FROM store_sales
WHERE store_id IN (2, 4)
GROUP BY store_id
ORDER BY units_range
