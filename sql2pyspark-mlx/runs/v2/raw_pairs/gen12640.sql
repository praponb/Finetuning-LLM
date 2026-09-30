SELECT id, amount, ROUND(amount, 2) AS amount_rounded FROM orders WHERE store_id IS NOT NULL AND order_date < '2024-05-01' ORDER BY amount DESC, amount_rounded DESC;
