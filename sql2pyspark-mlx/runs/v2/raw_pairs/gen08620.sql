SELECT id, product_id, order_id, unit_price, ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY unit_price, id) AS unit_price_row FROM order_items
