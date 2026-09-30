SELECT
    id,
    browser,
    country,
    customer_id,
    INITCAP(TRIM(country)) AS country_title
FROM sessions
WHERE started_at >= '2024-08-05'
ORDER BY customer_id DESC, id
LIMIT 5;
