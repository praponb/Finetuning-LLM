SELECT
    id,
    amount,
    order_id,
    TRIM(status) clean_status
FROM payments
WHERE status NOT IN ('REFUNDED', 'FAILED', 'SUCCESS') AND method = 'paypal' AND status = 'REFUNDED'
ORDER BY order_id, id
LIMIT 5;
