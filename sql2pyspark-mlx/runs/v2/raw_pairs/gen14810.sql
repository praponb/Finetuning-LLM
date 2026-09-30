SELECT id, order_id, product_id, unit_price, ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY unit_price, id) AS unit_price_row FROM order_items
