SELECT
    QUARTER(order_date) AS order_quarter,
    AVG(discount) AS avg_discount,
    ROUND(STDDEV(discount), 2) AS stddev_discount,
    SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_count
FROM orders
GROUP BY QUARTER(order_date)
ORDER BY order_quarter DESC;
