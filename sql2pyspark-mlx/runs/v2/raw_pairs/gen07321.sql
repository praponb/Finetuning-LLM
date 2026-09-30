WITH recent_payments AS (
    SELECT *, SUBSTRING(status, 1, 4) AS status_prefix
    FROM payments
    WHERE method NOT IN ('promptpay', 'bank_transfer', 'card') AND method IN ('card', 'promptpay', 'cash')
),
agg AS (
    SELECT status, COUNT(*) AS cnt
    FROM recent_payments
    GROUP BY status
)
SELECT status, cnt
FROM agg
WHERE cnt < 4;
