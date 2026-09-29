SELECT country, device, customer_id
FROM sessions
WHERE device = 'tablet' AND customer_id IN (6, 35)
