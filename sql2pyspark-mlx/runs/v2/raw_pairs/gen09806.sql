SELECT
    id,
    order_id,
    paid_at,
    status,
    TRIM(method) AS clean_method
FROM payments
WHERE paid_at >= '2024-08-01' AND method = 'bank_transfer'
ORDER BY 1 DESC, status DESC
