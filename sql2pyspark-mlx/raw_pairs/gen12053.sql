SELECT o.store_id, o.order_date, o.amount, p.method, p.order_id FROM orders o INNER JOIN payments p ON o.id = p.order_id WHERE o.amount BETWEEN 500 AND 1130;
