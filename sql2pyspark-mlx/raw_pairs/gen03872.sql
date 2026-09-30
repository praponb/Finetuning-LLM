SELECT QUARTER(order_date) AS order_quarter, MAX(order_date) AS latest_order_date, MIN(order_date) AS first_order_date
FROM orders
WHERE amount <= 1580 OR store_id = 39
GROUP BY QUARTER(order_date)
ORDER BY latest_order_date
