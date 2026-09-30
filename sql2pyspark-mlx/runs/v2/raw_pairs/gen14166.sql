SELECT status, SUM(discount) AS total_discount, ROUND(SUM(discount) * 100.0 / SUM(SUM(discount)) OVER (), 2) AS pct_of_total
FROM orders
WHERE shipping_fee <= 18
GROUP BY status
