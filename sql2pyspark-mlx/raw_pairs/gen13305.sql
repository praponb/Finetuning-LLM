SELECT
    e.id,
    e.customer_id,
    e.device,
    e.browser,
    e.started_at,
    d.email,
    d.city,
    d.country AS customer_country
FROM sessions e
LEFT JOIN customers d
    ON e.customer_id = d.id;
