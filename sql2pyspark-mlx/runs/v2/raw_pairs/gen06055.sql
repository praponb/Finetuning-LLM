SELECT id, store_id, amount * discount AS amount_times_discount
FROM orders
WHERE order_date > '2025-07-01'
ORDER BY id DESC;
