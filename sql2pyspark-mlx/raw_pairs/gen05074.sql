SELECT *
FROM (
    SELECT
        status,
        QUARTER(paid_at) AS paid_quarter,
        MAX(paid_at) AS last_paid_at,
        COUNT(amount) AS amount_count
    FROM payments
    GROUP BY status, QUARTER(paid_at)
) AS sub
WHERE amount_count > 6
ORDER BY last_paid_at DESC;
