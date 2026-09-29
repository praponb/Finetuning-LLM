SELECT started_at, browser, INITCAP(TRIM(country)) AS country_title
FROM sessions
WHERE started_at >= '2024-04-01'
