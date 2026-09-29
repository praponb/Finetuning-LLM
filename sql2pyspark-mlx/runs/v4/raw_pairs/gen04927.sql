SELECT browser, country
FROM sessions
EXCEPT ALL
SELECT browser, country
FROM sessions
WHERE country IN ('DE', 'US', 'TH');
