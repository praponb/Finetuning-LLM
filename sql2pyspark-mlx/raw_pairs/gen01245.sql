SELECT
    s.status,
    s.delivered_date,
    o.shipping_fee,
    o.customer_id,
    o.store_id
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
WHERE s.carrier IN ('FedEx', 'Flash', 'UPS') AND s.order_id = 22
