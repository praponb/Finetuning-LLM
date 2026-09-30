SELECT s.id, s.device, s.browser, s.started_at FROM sessions s WHERE s.customer_id IN (SELECT id FROM customers WHERE country IN ('TH', 'JP', 'UK')) ORDER BY device DESC
