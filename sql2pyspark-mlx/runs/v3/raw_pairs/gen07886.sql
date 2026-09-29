SELECT id, device
FROM sessions
WHERE started_at >= '2024-12-09' AND customer_id IN (9, 27);
