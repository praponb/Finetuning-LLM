SELECT o.store_id, s.delivered_date
FROM orders o
LEFT JOIN shipments s ON o.id = s.order_id
WHERE o.customer_id IS NULL OR o.discount BETWEEN 0.25 AND 0.31;
