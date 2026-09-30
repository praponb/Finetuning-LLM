SELECT product_id, MAX(units) - MIN(units) AS units_range
FROM store_sales
WHERE product_id IN (1, 3, 9) AND revenue > 1900
GROUP BY product_id
HAVING units_range > 20;
