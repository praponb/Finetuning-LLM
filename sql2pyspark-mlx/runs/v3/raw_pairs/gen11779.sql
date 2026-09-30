SELECT o.shipping_fee, o.customer_id, o.order_date, p.amount, p.status, p.method FROM orders o LEFT JOIN payments p ON o.id = p.order_id ORDER BY p.amount;
