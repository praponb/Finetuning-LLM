SELECT p.amount, p.paid_at, o.store_id, s.region FROM payments p JOIN orders o ON o.id = p.order_id LEFT OUTER JOIN stores s ON o.store_id = s.id WHERE o.amount IS NOT NULL;
