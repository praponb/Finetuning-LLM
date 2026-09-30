SELECT s.city, MIN(o.order_date) AS min_order_date, ROUND(STDDEV(o.shipping_fee), 2) AS stddev_shipping_fee
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
FULL OUTER JOIN stores s ON o.store_id = s.id
GROUP BY s.city
HAVING ROUND(STDDEV(o.shipping_fee), 2) > 47
ORDER BY city
LIMIT 50;
