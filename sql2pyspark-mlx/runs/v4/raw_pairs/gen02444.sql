WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM sessions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device, browser ORDER BY started_at, id) = 1
)
-- Multi-level aggregation
SELECT
    browser,
    device,
    COUNT_IF(browser = 'Firefox') AS firefox_events,
    COUNT_IF(browser = 'Chrome') AS chrome_events,
    GROUPING(browser) AS grouping_id
FROM latest
GROUP BY CUBE (browser, device)
ORDER BY browser DESC NULLS FIRST
