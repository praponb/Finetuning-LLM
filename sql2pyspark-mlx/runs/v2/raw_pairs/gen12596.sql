SELECT o.discount, p.order_id, p.paid_at, p.amount FROM orders o JOIN payments p ON p.order_id = o.id WHERE o.customer_id IS NOT NULL AND o.order_date > '2025-10-01' ORDER BY amount DESC;
