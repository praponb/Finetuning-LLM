SELECT sale_date
FROM store_sales
WHERE product_id IN (2, 5, 6, 8) AND product_id IN (1, 4, 10)
ORDER BY sale_date DESC
