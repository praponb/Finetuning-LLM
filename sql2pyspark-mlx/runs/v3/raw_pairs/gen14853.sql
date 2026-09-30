SELECT o.store_id, o.customer_id, s.shipped_date
FROM orders o
LEFT OUTER JOIN shipments s ON o.id = s.order_id
WHERE o.store_id = 8
ORDER BY store_id DESC, o.customer_id DESC;
