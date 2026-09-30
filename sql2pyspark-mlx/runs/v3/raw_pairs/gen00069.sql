WITH filtered_payments AS (
    SELECT *
    FROM payments
    WHERE paid_at < '2023-10-24' AND method <> 'bank_transfer' AND status NOT IN ('SUCCESS', 'FAILED', 'REFUNDED')
),
paid_month_summary AS (
    SELECT DATE_TRUNC('MONTH', paid_at) AS paid_month, COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_count, MAX(paid_at) AS last_paid_at
    FROM filtered_payments
    GROUP BY DATE_TRUNC('MONTH', paid_at)
)
SELECT paid_month, failed_count, last_paid_at
FROM paid_month_summary
ORDER BY paid_month DESC;
