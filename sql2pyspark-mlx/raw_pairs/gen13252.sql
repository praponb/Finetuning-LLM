WITH joined AS (
    -- Attach sessions attributes
    SELECT
        s.id,
        s.session_id,
        d.browser,
        d.device
    FROM web_events s
    LEFT JOIN sessions d
        ON s.session_id = d.id
    WHERE d.browser = 'Edge'
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM joined
    EXCEPT ALL
    SELECT id
    FROM web_events
    WHERE duration_sec > 418
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN kept_keys k ON s.id = k.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT_WS(',', device, browser) AS device_browser_key,
        COALESCE(device, 'OTHER') AS device_filled
    FROM reconciled
    WHERE browser IS NOT NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN device = 'tablet' THEN 1 ELSE 0 END AS is_tablet,
        COALESCE(browser, 'N/A') AS browser_clean
    FROM filtered
    WHERE device IS NOT NULL
      AND session_id IN (12, 40)
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        device,
        browser,
        is_tablet
    FROM valid
) src
PIVOT (
    SUM(is_tablet) AS total,
    COUNT(1) AS item_count
    FOR browser IN ('Chrome', 'Firefox', 'Edge')
);
