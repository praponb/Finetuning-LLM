SELECT *, UPPER(country) AS country_upper, TRIM(device) AS clean_device FROM sessions WHERE TO_DATE(started_at) < '2024-03-16' ORDER BY started_at DESC, country_upper ASC;
