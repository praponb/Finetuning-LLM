SELECT id, unit_price, quantity, CAST(unit_price AS INT) AS unit_price_int FROM order_items WHERE product_id IN (10, 30) AND quantity = 7 AND product_id IS NULL ORDER BY id
