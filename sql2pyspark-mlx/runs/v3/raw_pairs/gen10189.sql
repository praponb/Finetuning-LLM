SELECT country
FROM sessions
WHERE country = 'US'
EXCEPT
SELECT country
FROM sessions
WHERE browser = 'Chrome';
