SELECT s.id, COUNT(o.id) order_count, AVG(o.shipping_fee) avg_shipping_fee
FROM stores s
LEFT OUTER JOIN orders o ON s.id = o.store_id
WHERE o.customer_id IN (27, 29)
GROUP BY s.id
ORDER BY avg_shipping_fee DESC, id
LIMIT 5;
