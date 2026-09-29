SELECT s.city, ROUND(AVG(o.shipping_fee), 2) AS avg_shipping_fee, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(o.status))) AS status_values
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
WHERE MONTH(o.order_date) IN (2, 5, 10)
GROUP BY s.city;
