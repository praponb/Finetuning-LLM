SELECT
    amount_bucket,
    method,
    num_payments,
    min_amount
FROM (
    SELECT
        CASE WHEN amount >= 1290 THEN 'high' WHEN amount >= 440 THEN 'mid' ELSE 'low' END amount_bucket,
        method,
        COUNT(*) num_payments,
        MIN(amount) min_amount
    FROM payments
    WHERE status = 'SUCCESS'
    GROUP BY CASE WHEN amount >= 1290 THEN 'high' WHEN amount >= 440 THEN 'mid' ELSE 'low' END, method
) t
WHERE num_payments >= 8
ORDER BY amount_bucket
