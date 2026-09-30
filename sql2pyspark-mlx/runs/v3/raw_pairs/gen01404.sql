WITH base AS (
    SELECT *, COALESCE(amount, 0) AS amount_filled
    FROM payments
    WHERE method = 'bank_transfer'
),
paid_month_summary AS (
    SELECT DATE_FORMAT(paid_at, 'yyyy-MM') AS paid_month, MAX(amount_filled) - MIN(amount_filled) AS amount_filled_range, MIN(amount) AS min_amount
    FROM base
    GROUP BY DATE_FORMAT(paid_at, 'yyyy-MM')
)
SELECT *
FROM paid_month_summary
WHERE min_amount >= 510
ORDER BY amount_filled_range DESC;
