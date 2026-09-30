SELECT order_id, CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END AS is_delivered
FROM shipments
WHERE carrier = 'UPS';
