SELECT id, device
FROM sessions
WHERE browser = 'Edge'
EXCEPT
SELECT id, device
FROM sessions
WHERE device = 'mobile'
ORDER BY device
