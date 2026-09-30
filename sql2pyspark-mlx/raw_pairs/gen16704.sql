WITH base AS (
    SELECT *, ROUND(amount, 0) AS amount_rounded
    FROM payments
    WHERE status = 'REFUNDED'
)
SELECT *
FROM (
    SELECT
        order_id,
        method,
        amount
    FROM base
) src
PIVOT (
    AVG(amount)
    FOR method IN ('promptpay', 'paypal', 'bank_transfer')
);
