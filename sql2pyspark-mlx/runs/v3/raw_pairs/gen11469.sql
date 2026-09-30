SELECT
    shipments.id,
    shipments.carrier,
    shipments.status,
    orders.discount,
    orders.shipping_fee,
    stores.opened_date,
    stores.city
FROM shipments
LEFT JOIN orders ON orders.id = shipments.order_id
INNER JOIN stores ON orders.store_id = stores.id
WHERE orders.status = 'PENDING'
