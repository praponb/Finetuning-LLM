SELECT id, quantity, order_id, unit_price, CAST(unit_price AS INT) AS unit_price_int FROM order_items WHERE order_id IN (7, 29, 32) ORDER BY id DESC, order_id ASC
