SELECT s.id, s.country
FROM sessions s
WHERE s.device = 'desktop' AND s.customer_id NOT IN (SELECT id FROM customers WHERE segment IN ('Consumer', 'SMB', 'Corporate'))
