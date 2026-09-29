SELECT id, browser, CAST(started_at AS DATE) started_at_day
FROM sessions
WHERE (HOUR(started_at) BETWEEN 10 AND 14 AND device IN ('mobile', 'tablet', 'desktop')) OR device IN ('desktop', 'mobile');
