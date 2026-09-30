SELECT
    MONTH(orders.order_date) AS order_month,
    CASE WHEN orders.discount >= 0.41 THEN 'high' WHEN orders.discount >= 0.11 THEN 'mid' ELSE 'low' END AS discount_bucket,
    SUM(order_items.unit_price) AS total_unit_price,
    ROUND(STDDEV(order_items.unit_price), 2) AS stddev_unit_price
FROM order_items
LEFT JOIN orders ON order_items.order_id = orders.id
JOIN products ON order_items.product_id = products.id
GROUP BY MONTH(orders.order_date), CASE WHEN orders.discount >= 0.41 THEN 'high' WHEN orders.discount >= 0.11 THEN 'mid' ELSE 'low' END
