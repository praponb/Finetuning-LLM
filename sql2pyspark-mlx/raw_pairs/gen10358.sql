SELECT started_at, browser, country, TO_DATE(started_at) started_at_date FROM sessions WHERE started_at >= '2025-10-05' ORDER BY country;
