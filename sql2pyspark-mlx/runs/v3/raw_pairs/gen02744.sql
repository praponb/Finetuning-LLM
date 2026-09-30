SELECT
    id,
    status,
    paid_at,
    order_id
FROM payments
WHERE amount > 720
ORDER BY id;
