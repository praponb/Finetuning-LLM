SELECT
    shipments.carrier,
    shipments.delivered_date,
    shipments.status,
    orders.channel,
    orders.amount,
    orders.shipping_fee,
    COALESCE(shipments.carrier, 'Unknown') AS carrier_filled
FROM shipments
LEFT JOIN orders ON orders.id = shipments.order_id
WHERE shipments.carrier IN ('DHL', 'Kerry', 'UPS');
