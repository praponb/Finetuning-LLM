SELECT
    id,
    status,
    order_id,
    delivered_date,
    CASE WHEN carrier = 'FedEx' THEN 1 ELSE 0 END AS is_fedex
FROM shipments
WHERE status = 'LOST';
