SELECT p.paid_at, p.order_id, o.order_date, COALESCE(p.amount, 0) amount_filled FROM payments p LEFT JOIN orders o ON p.order_id = o.id ORDER BY o.order_date DESC
