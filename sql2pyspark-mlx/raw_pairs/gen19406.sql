SELECT s.id, s.started_at, s.browser, s.device FROM sessions s WHERE s.country IS NOT NULL AND s.customer_id IN (SELECT id FROM customers WHERE is_active)
