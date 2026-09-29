SELECT
    customer_id,
    started_at,
    device,
    INITCAP(TRIM(country)) AS country_title
FROM sessions
WHERE browser IN ('Firefox', 'Chrome');
