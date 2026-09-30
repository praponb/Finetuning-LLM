SELECT
    id,
    country,
    device,
    DATE_FORMAT(started_at, 'MMM yyyy') AS started_at_label,
    INITCAP(TRIM(country)) AS country_title
FROM sessions
WHERE NOT (customer_id IS NOT NULL OR country IN ('SG', 'DE'))
