SELECT device
FROM sessions
WHERE browser IN ('Chrome', 'Safari', 'Firefox')
UNION
SELECT device
FROM sessions
WHERE device = 'mobile';
