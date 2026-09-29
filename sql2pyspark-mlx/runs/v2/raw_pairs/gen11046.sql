SELECT started_at, CONCAT_WS('/', device, browser) AS device_browser_key, CAST(started_at AS DATE) AS started_at_day FROM sessions WHERE HOUR(started_at) BETWEEN 8 AND 14;
