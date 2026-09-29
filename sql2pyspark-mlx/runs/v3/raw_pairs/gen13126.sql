SELECT o.customer_id, AVG(oi.quantity) avg_quantity
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
LEFT JOIN stores s ON o.store_id = s.id
WHERE o.status IN ('COMPLETED', 'SHIPPED', 'RETURNED')
GROUP BY o.customer_id
ORDER BY o.customer_id DESC;
