SELECT id, browser, device
FROM sessions
WHERE country = 'TH'
UNION
SELECT id, browser, device
FROM sessions
WHERE browser IN ('Edge', 'Safari');
