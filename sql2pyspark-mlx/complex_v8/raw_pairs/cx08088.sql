WITH recent_sites AS (
    -- Scope the sites rows
    SELECT
        id,
        zone,
        tariff,
        installed_kw,
        commissioned_on
    FROM sites
    WHERE commissioned_on >= DATE '2024-08-24'
      AND tariff = 'TOU'
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN installed_kw >= 284.0 THEN 'A' WHEN installed_kw >= 96.9 THEN 'B' ELSE 'C' END AS installed_kw_tier,
        FLOOR(installed_kw / 100) * 100 AS installed_kw_bucket
    FROM recent_sites
    WHERE commissioned_on BETWEEN DATE '2024-08-24' AND DATE '2025-12-03'
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT_WS('|', installed_kw_tier, zone) AS installed_kw_tier_zone_label,
        ABS(installed_kw_bucket - 154) AS installed_kw_bucket_distance,
        installed_kw_bucket + installed_kw AS installed_kw_bucket_plus_installed_kw
    FROM tagged
)
SELECT
    COALESCE(zone, 'ALL') AS zone,
    COALESCE(installed_kw_tier, 'ALL') AS installed_kw_tier,
    COUNT(*) AS cnt
FROM scored
GROUP BY CUBE(zone, installed_kw_tier)
ORDER BY zone NULLS LAST, installed_kw_tier NULLS LAST
