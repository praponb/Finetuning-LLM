SELECT DISTINCT browser, country
FROM sessions
WHERE country != 'DE';
