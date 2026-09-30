SELECT sale_date
FROM store_sales
WHERE product_id IN (2, 6, 10)
EXCEPT
SELECT delivered_date
FROM shipments
