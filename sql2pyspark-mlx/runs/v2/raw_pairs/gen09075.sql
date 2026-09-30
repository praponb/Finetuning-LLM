SELECT p.id, p.status
FROM payments p
WHERE p.status = 'SUCCESS' AND p.order_id IN (SELECT id FROM orders WHERE shipping_fee IS NOT NULL);
