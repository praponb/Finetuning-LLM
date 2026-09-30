SELECT id, order_id, unit_price, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) AS row_num FROM order_items
