SELECT status, SUM(discount) AS total_discount, SUM(SUM(discount)) OVER (ORDER BY status) AS cumulative_discount
FROM orders
WHERE amount >= 700
GROUP BY status
ORDER BY status, total_discount DESC
