SELECT product_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity
FROM order_items
GROUP BY product_id
ORDER BY stddev_quantity DESC;
