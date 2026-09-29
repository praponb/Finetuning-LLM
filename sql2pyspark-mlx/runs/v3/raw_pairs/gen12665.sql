SELECT product_id, ROUND(STDDEV(unit_price), 2) AS stddev_unit_price
FROM order_items
WHERE order_id IN (9, 25) AND quantity < 7
GROUP BY product_id
ORDER BY product_id DESC;
