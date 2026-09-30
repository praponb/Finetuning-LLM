SELECT orders.order_date, shipments.cost, ROUND(orders.amount * 0.9, 2) AS amount_adjusted
FROM orders
LEFT JOIN shipments ON shipments.order_id = orders.id
ORDER BY amount_adjusted DESC
