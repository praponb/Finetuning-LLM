WITH scoped AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        read_at + INTERVAL 1 DAYS AS read_at_plus_1d
    FROM meter_readings
),
with_details AS (
    -- Attach site attributes
    SELECT s.*, si.installed_kw AS site_installed_kw, si.tariff AS site_tariff
    FROM scoped s
    JOIN sites si ON si.id = s.site_id
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(site_tariff, 'ALL') AS site_tariff,
    site_id,
    GROUPING_ID() AS gid,
    PERCENTILE_APPROX(kwh, 0.5) AS median_kwh,
    ARRAY_SORT(COLLECT_SET(site_tariff)) AS site_tariff_list,
    MIN(read_at_plus_1d) AS first_read_at_plus_1d
FROM with_details
GROUP BY GROUPING SETS ((site_tariff, site_id), (site_tariff))
