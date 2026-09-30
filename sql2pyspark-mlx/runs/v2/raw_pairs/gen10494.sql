SELECT o.id, o.shipping_fee, o.amount FROM orders o WHERE o.store_id IS NOT NULL AND o.customer_id NOT IN (SELECT id FROM customers WHERE country NOT IN ('US', 'TH'))
