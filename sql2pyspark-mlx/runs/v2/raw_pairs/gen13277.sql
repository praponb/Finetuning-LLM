SELECT c.email, o.store_id, o.order_date FROM customers c LEFT JOIN orders o ON c.id = o.customer_id WHERE c.name LIKE 'Ol%' AND o.channel IN ('store', 'mobile', 'partner')
