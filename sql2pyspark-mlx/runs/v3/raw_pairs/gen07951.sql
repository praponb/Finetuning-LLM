SELECT
    shipments.id,
    shipments.cost,
    shipments.order_id,
    shipments.delivered_date,
    orders.customer_id
FROM shipments
INNER JOIN orders ON orders.id = shipments.order_id
WHERE orders.amount BETWEEN 1130 AND 1400 AND orders.status != 'PENDING'
ORDER BY 1
LIMIT 10
