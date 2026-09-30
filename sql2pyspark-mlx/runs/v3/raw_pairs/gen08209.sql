SELECT id, status
FROM orders
WHERE channel IN ('store', 'mobile', 'partner') AND (customer_id IN (13, 22, 31, 39) OR customer_id = 13);
