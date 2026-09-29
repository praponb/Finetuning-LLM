SELECT order_date AS event_date
FROM orders
EXCEPT
SELECT shipped_date AS event_date
FROM shipments
WHERE cost > 71
