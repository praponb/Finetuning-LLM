SELECT id, browser, customer_id, device FROM sessions WHERE NOT (browser NOT IN ('Chrome', 'Safari') AND browser = 'Firefox' AND started_at < '2024-08-01') ORDER BY id DESC;
