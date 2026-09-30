SELECT
    id,
    started_at,
    country,
    ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at, id) AS row_seq,
    ROW_NUMBER() OVER (ORDER BY started_at, id) AS event_seq,
    LAST_VALUE(country) IGNORE NULLS OVER (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country
FROM sessions;
