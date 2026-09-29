WITH filtered_payments AS (
    SELECT *
    FROM payments
    WHERE method = 'cash'
),
paid_month_summary AS (
    SELECT
        MONTH(paid_at) AS paid_month,
        status,
        MAX(amount) - MIN(amount) AS amount_range,
        MAX(paid_at) AS last_paid_at
    FROM filtered_payments
    GROUP BY MONTH(paid_at), status
)
SELECT
    paid_month,
    status,
    amount_range,
    last_paid_at
FROM paid_month_summary;
