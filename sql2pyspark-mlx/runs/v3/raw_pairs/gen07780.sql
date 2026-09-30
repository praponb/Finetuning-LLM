SELECT c.is_active, s.status FROM customers c LEFT JOIN subscriptions s ON s.customer_id = c.id WHERE s.status IN ('ACTIVE', 'CANCELLED') AND c.segment = 'Consumer';
