SELECT
    id,
    started_at,
    FIRST_VALUE(country, TRUE) OVER (PARTITION BY customer_id, country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country
FROM sessions
WHERE browser = 'Edge'
