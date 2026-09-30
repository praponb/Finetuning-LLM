SELECT
    DATE_TRUNC('MONTH', order_date) AS order_month,
    status,
    ROUND(STDDEV(discount), 2) AS stddev_discount,
    SUM(discount) AS total_discount
FROM orders
WHERE channel IS NOT NULL
GROUP BY 1, 2
HAVING SUM(discount) > 1.2
ORDER BY 2 ASC, order_month ASC
LIMIT 10;
