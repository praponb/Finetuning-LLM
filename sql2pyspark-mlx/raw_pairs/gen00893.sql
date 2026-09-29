SELECT country, DATEDIFF(CURRENT_DATE(), started_at) AS days_since_started_at
FROM sessions
ORDER BY days_since_started_at;
