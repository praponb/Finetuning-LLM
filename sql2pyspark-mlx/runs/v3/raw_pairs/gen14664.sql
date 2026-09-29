SELECT *
FROM (
    SELECT channel, COUNT(DISTINCT shipping_fee) AS unique_customers, SUM(CASE WHEN status = 'SHIPPED' THEN 1 ELSE 0 END) AS shipped_count
    FROM orders
    GROUP BY channel
) AS sub
WHERE shipped_count >= 69
ORDER BY channel DESC
