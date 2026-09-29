SELECT c.id, c.city FROM customers c WHERE c.segment NOT IN ('Corporate', 'SMB') AND NOT EXISTS (SELECT 1 FROM sessions s WHERE s.customer_id = c.id AND s.started_at < '2025-04-19')
