WITH filtered_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN quality IN ('OK', 'BAD') THEN 'group_a' WHEN quality = 'EST' THEN 'group_b' ELSE 'other' END AS quality_group,
        DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label
    FROM meter_readings
),
with_parent_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.quality_group,
        s.read_at_label,
        si.zone AS site_zone,
        si.tariff AS site_tariff
    FROM filtered_readings s
    LEFT JOIN sites si ON si.id = s.site_id
),
rollup_base AS (
    -- Group-level totals
    SELECT
        site_zone,
        site_id,
        COUNT(DISTINCT site_zone) AS distinct_site_zones,
        ROUND(SUM(kwh), 2) AS total_kwh,
        ROUND(STDDEV(voltage), 2) AS stddev_voltage,
        ROUND(100 * COUNT(CASE WHEN site_zone IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_site_zone_is_not_null
    FROM with_parent_site
    WHERE voltage >= 214.3
    GROUP BY site_zone, site_id
    HAVING SUM(voltage) > 216.8
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        site_zone,
        site_id,
        distinct_site_zones,
        total_kwh,
        stddev_voltage,
        pct_site_zone_is_not_null,
        NTILE(10) OVER (PARTITION BY distinct_site_zones ORDER BY total_kwh DESC, site_zone, site_id) AS total_kwh_decile,
        SUM(distinct_site_zones) OVER (PARTITION BY site_id) AS site_id_sum_distinct_site_zones
    FROM rollup_base
),
line_level AS (
    -- One row per reading
    SELECT
        s.site_zone,
        s.site_id,
        s.total_kwh,
        s.stddev_voltage,
        s.pct_site_zone_is_not_null,
        s.total_kwh_decile,
        s.site_id_sum_distinct_site_zones,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.voltage AS reading_voltage,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality
    FROM windowed s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id AND m.read_at < TIMESTAMP '2025-02-24 10:00:00'
)
-- Subtotals with CUBE
SELECT
    reading_quality,
    site_zone,
    COUNT(*) AS row_count,
    ROUND(SUM(reading_voltage), 2) AS total_reading_voltage,
    COUNT(DISTINCT reading_id) AS distinct_reading_ids
FROM line_level
GROUP BY CUBE(reading_quality, site_zone)
ORDER BY reading_quality NULLS LAST, site_zone NULLS LAST
