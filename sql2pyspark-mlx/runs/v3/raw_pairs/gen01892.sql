SELECT QUARTER(o.order_date) AS order_quarter, ROUND(STDDEV(oi.quantity), 2) AS stddev_quantity
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
WHERE o.discount < 0.14
GROUP BY QUARTER(o.order_date);
