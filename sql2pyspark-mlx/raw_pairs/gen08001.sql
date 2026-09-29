SELECT id, product_id, revenue * units AS revenue_per_unit
FROM store_sales
WHERE product_id IN (2, 7) AND store_id IN (1, 4, 6);
