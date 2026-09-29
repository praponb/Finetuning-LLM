SELECT
    id,
    status,
    amount,
    order_id,
    CAST(paid_at AS DATE) AS paid_at_day,
    TRIM(method) AS clean_method
FROM payments
WHERE amount BETWEEN 1140 AND 1190
