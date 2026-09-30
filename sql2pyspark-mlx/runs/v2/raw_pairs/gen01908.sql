SELECT s.id, s.monthly_fee
FROM subscriptions s
WHERE MONTH(s.start_date) IN (4, 8, 10) AND s.customer_id NOT IN (SELECT id FROM customers WHERE UPPER(TRIM(email)) = 'IVY@GMAIL.COM');
