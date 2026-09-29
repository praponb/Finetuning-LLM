SELECT
    customer_id,
    SUM(discount) total_discount,
    COUNT(*) cnt,
    ROUND(SUM(discount) * 100.0 / SUM(SUM(discount)) OVER (), 2) AS pct_of_total
FROM orders
WHERE amount <= 710
GROUP BY customer_id
ORDER BY customer_id DESC
