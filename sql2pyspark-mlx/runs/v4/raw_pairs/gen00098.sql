SELECT *
FROM (
    SELECT YEAR(order_date) AS order_year, ROUND(STDDEV(amount), 2) AS stddev_amount, MAX(discount) AS max_discount
    FROM orders
    GROUP BY YEAR(order_date)
) AS s
WHERE stddev_amount >= 51
ORDER BY max_discount DESC
