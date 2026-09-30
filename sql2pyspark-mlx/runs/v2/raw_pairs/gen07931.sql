SELECT status, order_date, channel
FROM orders
WHERE order_date BETWEEN '2023-07-01' AND '2025-01-01' AND channel IN ('partner', 'web', 'mobile') AND discount BETWEEN 0.22 AND 0.26
