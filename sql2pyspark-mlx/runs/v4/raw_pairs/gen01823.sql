SELECT p.id, p.amount, o.discount, s.city FROM payments p INNER JOIN orders o ON p.order_id = o.id FULL OUTER JOIN stores s ON o.store_id = s.id WHERE TO_DATE(p.paid_at) < '2023-11-12'
