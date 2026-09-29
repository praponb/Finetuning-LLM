SELECT id, browser, customer_id, country, CAST(started_at AS DATE) AS started_at_day FROM sessions WHERE customer_id IN (5, 13, 16, 32) AND (customer_id = 37 OR started_at >= '2023-09-01');
