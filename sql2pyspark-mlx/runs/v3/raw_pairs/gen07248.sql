WITH recent_payments AS (
    SELECT *, SUBSTRING(status, 1, 4) AS status_prefix
    FROM payments
    WHERE method NOT IN ('promptpay', 'bank_transfer', 'card') AND method IN ('card', 'promptpay', 'cash')
),
agg AS (
    SELECT method, AVG(amount) AS avg_amount
    FROM recent_payments
    GROUP BY method
)
SELECT method, avg_amount
FROM agg
WHERE avg_amount < 590;
