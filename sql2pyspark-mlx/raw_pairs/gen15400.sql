SELECT id, store_id, units, NTILE(10) OVER (PARTITION BY store_id ORDER BY units, id) AS units_ntile FROM store_sales WHERE product_id IN (1, 2, 5, 7);
