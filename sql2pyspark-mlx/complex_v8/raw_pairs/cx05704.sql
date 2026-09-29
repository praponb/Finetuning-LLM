WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        CASE WHEN installed_kw >= 284.0 THEN 'A' WHEN installed_kw >= 96.9 THEN 'B' ELSE 'C' END AS installed_kw_tier,
        ADD_MONTHS(commissioned_on, 1) AS commissioned_on_plus_1m
    FROM sites
    WHERE tariff IS NOT NULL
      AND LOWER(site_name) LIKE '%m %'
),
tariff_stats AS (
    -- Avg installed_kw per tariff
    SELECT tariff, AVG(installed_kw) AS avg_installed_kw
    FROM base
    GROUP BY tariff
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_installed_kw,
        ROUND(s.installed_kw - g.avg_installed_kw, 2) AS installed_kw_minus_avg
    FROM base s
    JOIN tariff_stats g ON g.tariff = s.tariff
    WHERE s.installed_kw > g.avg_installed_kw
),
latest_per_key AS (
    -- Keep the earliest row per tariff, installed_kw_tier
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        installed_kw_tier,
        commissioned_on_plus_1m,
        avg_installed_kw,
        installed_kw_minus_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tariff, installed_kw_tier ORDER BY installed_kw, id) AS rn
        FROM outliers
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    site_name,
    commissioned_on,
    installed_kw_tier,
    commissioned_on_plus_1m,
    installed_kw_minus_avg,
    DAYOFWEEK(commissioned_on) AS commissioned_on_dayofweek
FROM latest_per_key
ORDER BY site_name DESC
