WITH recent_payments AS (
    SELECT *, DAYOFWEEK(paid_at) AS paid_at_dayofweek
    FROM payments
    WHERE amount > 1160 AND method = 'bank_transfer' AND amount <= 1370
),
payments_stats AS (
    SELECT status, MIN(paid_at_dayofweek) AS min_paid_at_dayofweek, MAX(paid_at_dayofweek) - MIN(paid_at_dayofweek) AS paid_at_dayofweek_range
    FROM recent_payments
    GROUP BY status
)
SELECT *
FROM payments_stats
WHERE paid_at_dayofweek_range >= 55
ORDER BY status DESC, min_paid_at_dayofweek DESC;
