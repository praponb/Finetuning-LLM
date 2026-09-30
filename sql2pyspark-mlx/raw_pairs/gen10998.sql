SELECT s.id, s.started_at, s.device FROM sessions s WHERE s.device = 'desktop' AND s.customer_id IN (SELECT id FROM customers WHERE country NOT IN ('US', 'SG', 'TH')) ORDER BY id LIMIT 10;
