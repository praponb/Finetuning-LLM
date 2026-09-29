SELECT s.id, s.monthly_fee FROM subscriptions s WHERE s.customer_id IN (SELECT id FROM customers WHERE segment NOT IN ('Consumer', 'SMB'));
