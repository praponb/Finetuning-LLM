WITH ValidPayments AS (
    SELECT *
    FROM payments
    WHERE amount >= 1570
),
payments_stats AS (
    SELECT DATE_TRUNC('MONTH', paid_at) AS paid_month, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(method))) AS method_values, COUNT(*) AS total_count
    FROM ValidPayments
    GROUP BY DATE_TRUNC('MONTH', paid_at)
)
SELECT paid_month, method_values, total_count
FROM payments_stats
WHERE total_count >= 4
