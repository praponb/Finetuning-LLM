SELECT product_id, units
FROM store_sales
WHERE product_id IN (1, 3, 5, 10)
EXCEPT
SELECT product_id, units
FROM store_sales
WHERE sale_date >= '2023-11-24';
