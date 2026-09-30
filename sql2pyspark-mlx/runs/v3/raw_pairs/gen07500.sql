SELECT o.id, o.discount, o.store_id FROM orders o WHERE o.customer_id IN (SELECT id FROM customers WHERE country <> 'UK');
