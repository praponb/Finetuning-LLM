SELECT
    id,
    status,
    store_id,
    customer_id,
    CASE WHEN status = 'RETURNED' THEN 1 ELSE 0 END is_returned
FROM orders
WHERE status = 'PENDING' AND channel IS NULL AND shipping_fee > 18;
