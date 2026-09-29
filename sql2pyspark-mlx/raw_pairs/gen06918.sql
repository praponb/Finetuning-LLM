SELECT id, browser, started_at, LENGTH(device) AS device_length FROM sessions WHERE country IN ('US', 'DE') AND HOUR(started_at) BETWEEN 10 AND 15;
