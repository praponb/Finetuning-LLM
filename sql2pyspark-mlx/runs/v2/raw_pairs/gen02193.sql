SELECT p.brand, ROUND(STDDEV(oi.unit_price), 2) stddev_unit_price FROM order_items oi INNER JOIN products p ON p.id = oi.product_id GROUP BY p.brand ORDER BY brand, stddev_unit_price;
