SELECT id, product_id, order_id, unit_price, RANK() OVER (PARTITION BY product_id, order_id ORDER BY unit_price) AS unit_price_rank FROM order_items WHERE order_id = 24
