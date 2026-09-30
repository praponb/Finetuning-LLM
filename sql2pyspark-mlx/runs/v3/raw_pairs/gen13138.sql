SELECT id, browser, started_at, device, FIRST_VALUE(device) OVER (PARTITION BY browser ORDER BY started_at DESC, id) AS first_device FROM sessions WHERE customer_id IS NULL;
