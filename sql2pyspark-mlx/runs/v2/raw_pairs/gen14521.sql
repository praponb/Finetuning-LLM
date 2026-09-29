SELECT id, customer_id, LENGTH(status) status_length FROM orders WHERE shipping_fee > 30 AND (amount >= 870 OR status IN ('CANCELLED', 'SHIPPED'));
