WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), started_at) / 30 AS months_since_started_at,
        CAST(started_at AS DATE) AS started_at_day
    FROM sessions
    WHERE country IS NOT NULL
      AND country IS NOT DISTINCT FROM 'DE'
)
-- Running and moving aggregates
SELECT
    id,
    customer_id,
    device,
    browser,
    started_at,
    months_since_started_at,
    started_at_day,
    NTILE(4) OVER (PARTITION BY customer_id ORDER BY months_since_started_at DESC, id) AS months_since_started_at_bucket,
    LAST_VALUE(browser) IGNORE NULLS OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser
FROM filtered
