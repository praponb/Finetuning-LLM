SELECT product_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity, SUM(quantity) AS total_quantity FROM inventory WHERE quantity > 221 GROUP BY product_id ORDER BY product_id DESC
