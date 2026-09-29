WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(country, 1, 4) AS country_prefix
    FROM sessions
    WHERE customer_id = 31
),
kept_keys AS (
    -- Set difference on keys
    SELECT device, country
    FROM cleaned
    EXCEPT DISTINCT
    SELECT device, country
    FROM sessions
    WHERE country = 'US'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.device = k.device AND s.country = k.country
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time >= '2025-02-01'
        )
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(country, 1, 3) AS country_prefix_2,
        CASE WHEN browser = 'Safari' THEN 1 ELSE 0 END AS is_safari
    FROM not_flagged
    WHERE started_at > TO_TIMESTAMP('2023-04-04 14:00:00')
      AND country IS DISTINCT FROM 'UK'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        browser,
        started_at,
        country,
        country_prefix,
        country_prefix_2,
        is_safari,
        AVG(is_safari) OVER (PARTITION BY MONTH(started_at), browser ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS is_safari_avg_30d,
        CUME_DIST() OVER w_part AS is_safari_cume_dist,
        AVG(is_safari) OVER (PARTITION BY browser) AS browser_avg_is_safari
    FROM filtered
    WHERE is_safari >= 37
    WINDOW w_part AS (PARTITION BY country, device ORDER BY is_safari NULLS FIRST)
)
SELECT
    id,
    browser,
    started_at,
    country_prefix,
    is_safari,
    is_safari_avg_30d,
    is_safari_cume_dist,
    browser_avg_is_safari,
    CASE WHEN browser RLIKE '^(Edge|Chrome)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS browser_zone
FROM metrics
ORDER BY is_safari_avg_30d DESC NULLS LAST
