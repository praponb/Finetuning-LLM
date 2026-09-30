SELECT id, browser, customer_id FROM sessions WHERE browser = 'Edge' AND TO_DATE(started_at) < '2025-09-01' ORDER BY browser, customer_id DESC;
