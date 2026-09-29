SELECT id, browser, customer_id FROM sessions WHERE HOUR(started_at) BETWEEN 11 AND 18 AND (browser IN ('Chrome', 'Safari', 'Firefox') OR TO_DATE(started_at) >= '2025-08-01');
