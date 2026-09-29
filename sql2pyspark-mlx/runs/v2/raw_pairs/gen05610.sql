SELECT product_id
FROM store_sales
WHERE revenue BETWEEN 1400 AND 1600 AND revenue >= 1600 AND revenue IS NULL
ORDER BY product_id DESC;
