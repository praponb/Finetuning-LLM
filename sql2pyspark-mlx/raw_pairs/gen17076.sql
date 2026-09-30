SELECT id, customer_id, country, device FROM sessions WHERE device = 'mobile' AND (browser = 'Edge' OR browser <> 'Safari') ORDER BY customer_id;
