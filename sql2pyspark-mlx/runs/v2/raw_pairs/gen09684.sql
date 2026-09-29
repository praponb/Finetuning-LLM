SELECT
    id,
    order_id,
    paid_at,
    method
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at DESC, id) rn
    FROM payments
    WHERE status IN ('SUCCESS', 'FAILED', 'REFUNDED')
) t
WHERE rn <= 3
ORDER BY id DESC;
