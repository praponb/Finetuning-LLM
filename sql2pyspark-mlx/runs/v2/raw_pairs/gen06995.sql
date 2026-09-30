SELECT started_at, COALESCE(country, 'Unknown') AS country_filled
FROM sessions
ORDER BY started_at;
