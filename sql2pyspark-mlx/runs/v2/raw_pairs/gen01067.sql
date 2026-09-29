SELECT ROUND(STDDEV(unit_price), 2) AS stddev_unit_price, SUM(unit_price) AS total_unit_price FROM order_items WHERE (order_id IN (3, 12, 22, 23) AND product_id IN (11, 20, 39)) OR quantity < 3
