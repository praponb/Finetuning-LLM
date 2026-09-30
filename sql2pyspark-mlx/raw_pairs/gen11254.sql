SELECT id, country
FROM sessions
WHERE country != 'JP' OR browser NOT IN ('Edge', 'Safari');
