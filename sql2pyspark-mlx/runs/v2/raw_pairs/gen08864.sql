SELECT
    status,
    QUARTER(order_date) AS order_quarter,
    MAX(discount) AS max_discount,
    COUNT(*) AS cnt,
    ROUND(STDDEV(amount), 2) AS stddev_amount
FROM orders
WHERE channel IN ('store', 'mobile') AND customer_id IN (12, 27, 34, 40)
GROUP BY 1, 2
HAVING ROUND(STDDEV(amount), 2) >= 38
ORDER BY order_quarter, status
LIMIT 5
