SELECT
    id,
    browser,
    started_at,
    country,
    LAST_VALUE(country) OVER (PARTITION BY browser ORDER BY started_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_country
FROM sessions;
