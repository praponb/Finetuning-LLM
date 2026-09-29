SELECT QUARTER(o.order_date) order_quarter, o.status, MAX(p.paid_at) last_paid_at
FROM payments p
LEFT JOIN orders o ON o.id = p.order_id
LEFT JOIN customers c ON o.customer_id = c.id
WHERE o.status IN ('CANCELLED', 'RETURNED', 'COMPLETED')
GROUP BY QUARTER(o.order_date), o.status;
