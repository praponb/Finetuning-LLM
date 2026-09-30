WITH scoped_sites AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        zone,
        installed_kw,
        commissioned_on,
        CASE WHEN installed_kw >= 284.0 THEN 'large' WHEN installed_kw >= 96.9 THEN 'medium' ELSE 'small' END AS installed_kw_tier,
        SUBSTRING(TRIM(zone), 1, 4) AS zone_prefix
    FROM sites
    WHERE zone IS NOT NULL
      AND commissioned_on >= DATE '2024-08-24'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        zone,
        installed_kw,
        commissioned_on,
        installed_kw_tier,
        ROUND(installed_kw / NULLIF(SUM(installed_kw) OVER w_commissioned_on_month, 0), 4) AS installed_kw_share_of_commissioned_on_month,
        NTH_VALUE(installed_kw, 2) OVER (PARTITION BY installed_kw_tier, zone ORDER BY installed_kw DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_installed_kw,
        MIN(installed_kw) OVER (PARTITION BY DATE_TRUNC('MONTH', commissioned_on)) AS commissioned_on_month_min_installed_kw,
        ROUND(CUME_DIST() OVER w_installed_kw_tier_zone_by_in_2, 3) AS installed_kw_cume_dist,
        FIRST_VALUE(commissioned_on) OVER (PARTITION BY installed_kw_tier, zone ORDER BY installed_kw DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_commissioned_on,
        SIZE(COLLECT_SET(installed_kw_tier) OVER w_commissioned_on_month) AS distinct_installed_kw_tier_in_commissioned_on_mo
    FROM scoped_sites
    WINDOW w_commissioned_on_month AS (PARTITION BY DATE_TRUNC('MONTH', commissioned_on)), w_installed_kw_tier_zone_by_in_2 AS (PARTITION BY installed_kw_tier, zone ORDER BY installed_kw DESC)
),
filtered AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM windowed s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.id
              AND m.voltage < 235.8
              AND m.quality = 'BAD'
        )
)
SELECT
    id,
    zone,
    installed_kw,
    commissioned_on,
    second_installed_kw,
    commissioned_on_month_min_installed_kw,
    installed_kw_cume_dist,
    first_commissioned_on,
    distinct_installed_kw_tier_in_commissioned_on_mo
FROM filtered
ORDER BY id, second_installed_kw DESC
