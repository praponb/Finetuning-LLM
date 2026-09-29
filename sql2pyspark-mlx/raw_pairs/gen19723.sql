SELECT country, browser FROM sessions WHERE started_at >= '2023-05-01' AND browser = 'Firefox' ORDER BY country DESC
