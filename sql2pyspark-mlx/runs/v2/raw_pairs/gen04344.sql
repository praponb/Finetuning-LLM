SELECT c.id, c.segment
FROM customers c
WHERE c.is_active AND EXISTS (SELECT 1 FROM subscriptions s WHERE s.customer_id = c.id AND YEAR(s.end_date) >= 2025);
