SELECT browser, device
FROM sessions
WHERE device = 'tablet'
EXCEPT
SELECT browser, device
FROM sessions
WHERE customer_id IN (2, 36);
