SELECT p.id, p.amount FROM payments p WHERE p.method IN ('bank_transfer', 'card') AND p.order_id NOT IN (SELECT id FROM orders WHERE shipping_fee >= 24);
