SELECT browser, started_at, device, HOUR(started_at) AS started_at_hour FROM sessions WHERE browser = 'Chrome' AND TO_DATE(started_at) < '2023-10-30';
