SELECT *, CASE WHEN browser = 'Safari' THEN 1 ELSE 0 END AS is_safari, CONCAT(country, ' - ', device) AS country_device FROM sessions
