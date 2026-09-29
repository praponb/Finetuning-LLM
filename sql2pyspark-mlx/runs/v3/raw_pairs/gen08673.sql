SELECT product_id, AVG(quantity) AS avg_quantity, ROUND(STDDEV(unit_price), 2) AS stddev_unit_price FROM order_items GROUP BY 1 HAVING AVG(quantity) >= 4.5;
