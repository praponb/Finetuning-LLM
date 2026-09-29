SELECT id, country
FROM sessions
WHERE browser <> 'Firefox' AND browser IN ('Edge', 'Safari');
