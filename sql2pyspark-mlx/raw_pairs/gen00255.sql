SELECT order_id, ROUND(STDDEV(quantity), 2) stddev_quantity
FROM order_items
WHERE unit_price BETWEEN 190 AND 350 OR quantity <= 7
GROUP BY order_id
