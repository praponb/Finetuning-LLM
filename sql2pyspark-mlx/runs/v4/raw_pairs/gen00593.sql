SELECT product_id, MAX(units) - MIN(units) AS units_range, ROUND(STDDEV(units), 2) AS stddev_units
FROM store_sales
WHERE revenue > 1300 OR store_id IN (2, 6)
GROUP BY product_id
ORDER BY product_id;
