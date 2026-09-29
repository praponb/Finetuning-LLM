SELECT id, product_id, units, MIN(units) OVER (PARTITION BY product_id) AS product_id_min_units FROM store_sales WHERE product_id IN (2, 7, 8)
