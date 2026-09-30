SELECT id, started_at, browser, COALESCE(country, 'Unknown') AS country_filled FROM sessions WHERE browser IN ('Edge', 'Chrome', 'Firefox') AND HOUR(started_at) BETWEEN 7 AND 15
