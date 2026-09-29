SELECT units
FROM store_sales
WHERE product_id IN (3, 9) AND product_id = 4 AND revenue <= 1100
ORDER BY 1 DESC;
