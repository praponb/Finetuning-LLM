SELECT id, browser, started_at, customer_id FROM sessions WHERE (device IN ('desktop', 'mobile', 'tablet') AND customer_id = 19) OR customer_id IN (17, 19, 28, 29);
