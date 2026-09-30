SELECT
    id,
    customer_id,
    device,
    browser,
    started_at,
    country
FROM sessions
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, browser ORDER BY started_at DESC NULLS FIRST, id) = 1
