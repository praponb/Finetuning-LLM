SELECT amount, channel, status, shipping_fee - amount AS amount_with_shipping FROM orders WHERE (shipping_fee < 26 AND order_date <= '2024-09-01') OR store_id IS NULL ORDER BY status
