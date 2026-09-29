SELECT country, device
FROM sessions
WHERE country IN ('DE', 'TH')
UNION
SELECT country, device
FROM sessions
WHERE device <> 'tablet';
