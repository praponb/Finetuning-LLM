WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        tariff,
        installed_kw
    FROM sites
),
with_activity AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM scoped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.id
              AND m.read_at < TIMESTAMP '2025-02-24 10:00:00'
        )
      AND s.tariff IS NOT NULL
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        site_name,
        tariff,
        installed_kw,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown,
        FLOOR(installed_kw / 100) * 100 AS installed_kw_bucket
    FROM with_activity
),
grouped AS (
    -- Group-level totals
    SELECT
        tariff_or_unknown,
        tariff,
        COUNT(DISTINCT tariff) AS distinct_tariffs,
        MAX(installed_kw) AS max_installed_kw
    FROM enriched
    WHERE installed_kw > 93.3
    GROUP BY 1, 2
)
-- Final projection
SELECT
    tariff_or_unknown,
    distinct_tariffs,
    max_installed_kw,
    ABS(distinct_tariffs - 6) AS distinct_tariffs_distance,
    CONCAT_WS('-', tariff_or_unknown, tariff) AS tariff_or_unknown_tariff_label
FROM grouped
ORDER BY distinct_tariffs, max_installed_kw
