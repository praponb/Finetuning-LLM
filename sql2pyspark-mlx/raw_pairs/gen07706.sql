SELECT
    shipments.id,
    shipments.cost,
    orders.amount,
    orders.discount,
    orders.status
FROM shipments
INNER JOIN orders ON shipments.order_id = orders.id
WHERE YEAR(shipments.shipped_date) >= 2024 OR shipments.order_id IS NOT NULL;
