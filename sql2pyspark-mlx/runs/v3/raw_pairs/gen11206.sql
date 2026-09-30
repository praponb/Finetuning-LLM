SELECT
    id,
    browser,
    started_at,
    country,
    ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at, id) rn,
    LAST_VALUE(country) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_country
FROM sessions
WHERE TO_DATE(started_at) >= '2024-06-25';
