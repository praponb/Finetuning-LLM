SELECT id, browser, device
FROM sessions
WHERE browser = 'Firefox' AND country = 'JP'
ORDER BY device, id DESC
LIMIT 5
