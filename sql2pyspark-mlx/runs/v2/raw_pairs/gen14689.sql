SELECT
    status,
    SUM(discount) AS total_discount,
    COUNT(*) AS cnt,
    SUM(SUM(discount)) OVER (ORDER BY status) AS cumulative_discount
FROM orders
WHERE status IN ('COMPLETED', 'SHIPPED', 'RETURNED')
GROUP BY status
