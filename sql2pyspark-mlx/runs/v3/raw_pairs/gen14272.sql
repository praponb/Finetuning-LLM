SELECT channel, ROUND(STDDEV(amount), 2) AS stddev_amount
FROM orders
WHERE NOT (MONTH(order_date) IN (8, 10, 12) AND store_id IN (3, 7, 14, 29) AND YEAR(order_date) >= 2023)
GROUP BY channel;
