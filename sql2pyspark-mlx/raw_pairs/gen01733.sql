SELECT *
FROM (
    SELECT QUARTER(order_date) AS order_quarter, COUNT(*) AS order_count, ROUND(STDDEV(discount), 2) AS stddev_discount
    FROM orders
    GROUP BY QUARTER(order_date)
) AS x
WHERE stddev_discount >= 63
ORDER BY order_count;
