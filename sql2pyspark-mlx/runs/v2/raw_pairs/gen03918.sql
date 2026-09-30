SELECT
    id,
    method,
    order_id,
    LOWER(status) AS status_lower
FROM payments
WHERE status != 'REFUNDED' AND method != 'bank_transfer';
