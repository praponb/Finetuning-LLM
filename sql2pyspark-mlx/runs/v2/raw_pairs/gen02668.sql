SELECT id, product_id, unit_price, DENSE_RANK() OVER (PARTITION BY product_id ORDER BY unit_price) unit_price_dense_rank FROM order_items WHERE quantity >= 7
