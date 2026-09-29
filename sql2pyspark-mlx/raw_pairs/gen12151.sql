SELECT id, started_at, INITCAP(TRIM(country)) AS country_title
FROM sessions
WHERE device = 'mobile' AND (customer_id = 38 OR customer_id IS NOT NULL)
