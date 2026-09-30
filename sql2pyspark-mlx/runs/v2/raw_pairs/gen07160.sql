SELECT id, product_id, units, DENSE_RANK() OVER (PARTITION BY product_id ORDER BY units DESC) AS units_dense_rank FROM store_sales WHERE store_id IN (1, 3, 5) ORDER BY units_dense_rank DESC;
