SELECT id, browser, device FROM sessions WHERE customer_id IS NULL UNION SELECT id, browser, device FROM sessions WHERE device = 'tablet' ORDER BY browser;
