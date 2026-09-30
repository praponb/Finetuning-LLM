SELECT id, customer_id, browser FROM sessions WHERE browser = 'Edge' UNION SELECT id, customer_id, browser FROM sessions WHERE device IN ('tablet', 'mobile', 'desktop') ORDER BY id;
