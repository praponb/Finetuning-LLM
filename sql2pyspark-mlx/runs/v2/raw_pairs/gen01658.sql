SELECT id, device, started_at, INITCAP(TRIM(device)) AS device_title FROM sessions WHERE NOT (country = 'TH' AND HOUR(started_at) BETWEEN 11 AND 17) ORDER BY started_at DESC, id DESC LIMIT 10;
