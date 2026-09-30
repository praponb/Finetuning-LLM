SELECT c.id, c.is_active
FROM customers c
WHERE c.segment NOT IN ('Consumer', 'SMB', 'Corporate') AND NOT EXISTS (SELECT 1 FROM subscriptions s WHERE s.customer_id = c.id AND s.monthly_fee <= 80);
