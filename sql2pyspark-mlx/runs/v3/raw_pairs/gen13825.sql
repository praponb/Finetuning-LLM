SELECT product_id, MAX(units) - MIN(units) AS units_range
FROM store_sales
WHERE units != 39 AND store_id IN (2, 5)
GROUP BY 1
ORDER BY units_range DESC;
