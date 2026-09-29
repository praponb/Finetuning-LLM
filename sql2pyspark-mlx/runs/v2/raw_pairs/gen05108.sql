SELECT p.id, p.order_id, p.amount
FROM payments p
WHERE p.method IN ('bank_transfer', 'promptpay', 'card') AND p.order_id IN (SELECT id FROM orders WHERE status = 'COMPLETED');
