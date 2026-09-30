SELECT order_id, product_id, COUNT(unit_price) unit_price_count, ROUND(STDDEV(unit_price), 2) stddev_unit_price FROM order_items GROUP BY CUBE(order_id, product_id) ORDER BY 3 DESC;
