SELECT
    shipments.delivered_date,
    shipments.carrier,
    shipments.order_id,
    orders.channel
FROM shipments
LEFT JOIN orders ON shipments.order_id = orders.id
WHERE orders.store_id IS NULL
ORDER BY carrier, delivered_date
