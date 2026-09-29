WITH base AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        CONCAT_WS('-', browser, country) AS browser_country_key,
        COALESCE(country, 'UNKNOWN') AS resolved_country
    FROM sessions
    WHERE device = 'desktop'
)
SELECT
    id,
    device,
    country,
    browser_country_key,
    resolved_country,
    ROW_NUMBER() OVER (PARTITION BY device, customer_id ORDER BY started_at DESC, id) AS row_seq,
    ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at, id) AS row_seq_2,
    FIRST_VALUE(country) IGNORE NULLS OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country
FROM base
WHERE browser IN ('Safari', 'Firefox', 'Edge')
