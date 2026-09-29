SELECT
    id,
    order_id,
    status,
    method
FROM payments
ORDER BY order_id DESC;
