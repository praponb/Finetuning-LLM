SELECT
    YEAR(order_date) AS order_year,
    DATE_FORMAT(order_date, 'yyyy-MM') AS order_month,
    ROUND(STDDEV(discount), 2) AS stddev_discount,
    COUNT(*) AS total_count,
    ROUND(AVG(amount), 2) AS avg_amount
FROM orders
WHERE shipping_fee >= 39
GROUP BY 1, 2;
