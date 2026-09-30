SELECT MIN(cost) AS min_cost
FROM shipments
WHERE order_id = 27 OR status <> 'RETURNED'
