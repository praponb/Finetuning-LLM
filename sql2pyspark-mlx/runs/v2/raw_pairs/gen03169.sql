SELECT p.id, p.amount, p.status, p.order_id, o.store_id, o.discount FROM payments p FULL OUTER JOIN orders o ON p.order_id = o.id WHERE o.status NOT IN ('SHIPPED', 'RETURNED');
