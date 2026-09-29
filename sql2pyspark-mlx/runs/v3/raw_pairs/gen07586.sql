SELECT order_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity, MAX(quantity) AS max_quantity FROM order_items WHERE quantity BETWEEN 3 AND 5 GROUP BY 1
