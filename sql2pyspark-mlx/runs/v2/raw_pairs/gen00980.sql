SELECT
    shipments.id,
    shipments.cost,
    shipments.order_id,
    shipments.status,
    orders.store_id,
    orders.discount,
    orders.order_date,
    CEIL(orders.amount) AS amount_whole
FROM shipments
INNER JOIN orders ON orders.id = shipments.order_id
ORDER BY shipments.id DESC
