SELECT
    id,
    customer_id,
    browser,
    started_at,
    country,
    LAST_VALUE(country, TRUE) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_country,
    ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('WEEK', started_at), country ORDER BY started_at, id) event_seq,
    FIRST_VALUE(browser, TRUE) OVER (PARTITION BY TO_DATE(started_at), device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_browser
FROM sessions
WHERE customer_id IN (4, 7, 18, 35)
