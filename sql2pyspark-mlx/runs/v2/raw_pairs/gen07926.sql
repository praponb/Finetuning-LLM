SELECT p.id, p.method, p.paid_at
FROM payments p
WHERE p.order_id IN (SELECT id FROM orders WHERE shipping_fee BETWEEN 30 AND 35)
ORDER BY paid_at;
