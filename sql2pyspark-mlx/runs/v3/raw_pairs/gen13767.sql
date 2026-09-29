SELECT s.id, s.start_date, s.monthly_fee FROM subscriptions s WHERE s.customer_id IN (5, 9) AND s.customer_id NOT IN (SELECT id FROM customers WHERE name NOT LIKE '%Sri%')
