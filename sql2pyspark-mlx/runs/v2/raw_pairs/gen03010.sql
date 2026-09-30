SELECT id, product_id, unit_price, NTILE(3) OVER (PARTITION BY product_id ORDER BY unit_price, id) AS unit_price_ntile FROM order_items WHERE product_id IN (29, 34)
