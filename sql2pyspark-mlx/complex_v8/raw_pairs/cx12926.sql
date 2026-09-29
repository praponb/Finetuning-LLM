WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        SUBSTRING(TRIM(zone), 1, 2) AS zone_prefix,
        DATEDIFF(DATE '2025-12-31', commissioned_on) AS days_since_commissioned_on,
        COALESCE(zone, 'UNKNOWN') AS zone_or_unknown
    FROM sites
    WHERE commissioned_on IS NOT NULL
),
deduped AS (
    -- Keep the latest row per zone_or_unknown, tariff
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        zone_prefix,
        days_since_commissioned_on,
        zone_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY zone_or_unknown, tariff ORDER BY days_since_commissioned_on DESC, id DESC) AS rn
        FROM prepared
    ) ranked
    WHERE rn = 1
)
-- Subtotals with ROLLUP
SELECT
    zone_or_unknown,
    tariff,
    zone,
    SUM(CASE WHEN tariff IS NOT NULL THEN 1 ELSE 0 END) AS n_tariff_is_not_null
FROM deduped
GROUP BY ROLLUP(zone_or_unknown, tariff, zone)
ORDER BY zone_or_unknown NULLS LAST, tariff NULLS LAST, zone NULLS LAST
