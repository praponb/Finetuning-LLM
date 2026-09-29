WITH base AS (
    SELECT *, CASE WHEN status IN ('SUCCESS', 'REFUNDED') THEN 'Group A' ELSE 'Other' END AS status_group
    FROM payments
    WHERE TO_DATE(paid_at) >= '2024-04-28'
)
SELECT
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    order_id,
    DATE_TRUNC('YEAR', paid_at) AS year_start,
    SUM(CASE WHEN method = 'promptpay' THEN amount ELSE 0 END) AS promptpay_amount,
    AVG(amount) FILTER (WHERE method = 'bank_transfer') AS bank_transfer_avg_amount,
    COUNT_IF(status = 'FAILED') AS failed_events,
    GROUPING(DATE_TRUNC('MONTH', paid_at)) AS gid
FROM base
GROUP BY ROLLUP (DATE_TRUNC('MONTH', paid_at), order_id, DATE_TRUNC('YEAR', paid_at))
HAVING COUNT_IF(status = 'FAILED') > 1
ORDER BY order_id DESC;
