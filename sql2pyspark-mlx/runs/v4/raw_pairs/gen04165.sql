WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        device,
        browser,
        country,
        CONCAT_WS(',', browser, country) AS browser_country_key,
        COALESCE(browser, 'OTHER') AS browser_filled
    FROM sessions
    WHERE country IS NOT NULL
      AND customer_id IS NOT NULL
),
kept_keys AS (
    -- Set difference on keys
    SELECT country, customer_id
    FROM filtered
    EXCEPT
    SELECT country, customer_id
    FROM sessions
    WHERE browser NOT IN ('Safari', 'Firefox', 'Chrome')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.country = k.country AND s.customer_id = k.customer_id
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.duration_sec <= 187
        )
),
filtered_sessions AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(browser) AS browser_length,
        COALESCE(country, 'N/A') AS country_filled
    FROM kept
    WHERE device IN ('mobile', 'tablet', 'desktop')
)
-- Multi-level aggregation
SELECT
    CASE WHEN browser_length >= 74 THEN 'HIGH' ELSE 'LOW' END AS browser_length_tier,
    COUNT(*) AS num_rows,
    COUNT(CASE WHEN browser = 'Chrome' THEN 1 END) AS chrome_count,
    AVG(browser_length) FILTER (WHERE country = 'JP') AS jp_avg_browser_length,
    SIZE(COLLECT_SET(country)) AS num_countries,
    COUNT(CASE WHEN country = 'TH' THEN 1 END) AS th_count
FROM filtered_sessions
GROUP BY CASE WHEN browser_length >= 74 THEN 'HIGH' ELSE 'LOW' END
HAVING COUNT(CASE WHEN country = 'TH' THEN 1 END) > 1
ORDER BY browser_length_tier DESC NULLS FIRST;
