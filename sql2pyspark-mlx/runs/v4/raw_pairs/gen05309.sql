SELECT s.id, s.status FROM subscriptions s WHERE MONTH(s.end_date) IN (3, 5, 6) AND s.customer_id IN (SELECT id FROM customers WHERE segment NOT IN ('SMB', 'Consumer'));
