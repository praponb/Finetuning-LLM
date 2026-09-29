SELECT order_id, ROUND(STDDEV(unit_price), 2) AS stddev_unit_price, COUNT(unit_price) AS unit_price_count FROM order_items GROUP BY 1
