SELECT
    status,
    SUM(discount) AS total_discount,
    COUNT(*) AS cnt,
    SUM(SUM(discount)) OVER (ORDER BY status) AS cumulative_discount
FROM orders
WHERE shipping_fee > 33
GROUP BY status
ORDER BY cumulative_discount DESC
