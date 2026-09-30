SELECT o.customer_id, o.store_id, s.shipped_date FROM orders o LEFT OUTER JOIN shipments s ON s.order_id = o.id WHERE s.status = 'RETURNED';
