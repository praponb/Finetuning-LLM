SELECT device, started_at, UPPER(country) AS country_upper
FROM sessions
WHERE browser IN ('Chrome', 'Edge', 'Firefox') AND country IN ('US', 'JP', 'UK')
