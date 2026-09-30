WITH base_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at,
        FLOOR(kwh / 25) * 25 AS kwh_bucket
    FROM meter_readings
    WHERE quality IN ('EST', 'OK')
      AND kwh <= 96.8
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(voltage, 2) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_voltage,
        NTILE(5) OVER (PARTITION BY quality ORDER BY read_at, id) AS read_at_quintile,
        ROUND(kwh_bucket - AVG(kwh_bucket) OVER (PARTITION BY quality), 2) AS kwh_bucket_vs_quality_avg,
        SUM(kwh_bucket) OVER (PARTITION BY quality) AS quality_sum_kwh_bucket
    FROM base_readings
),
combined_details_site AS (
    -- Join sites
    SELECT
        s.*,
        si.installed_kw AS site_installed_kw,
        si.tariff AS site_tariff,
        si.site_name
    FROM with_windows s
    JOIN sites si ON si.id = s.site_id
    WHERE si.installed_kw <= 386.6
),
site_id_stats AS (
    -- Avg read_at_quintile per site_id
    SELECT site_id, AVG(read_at_quintile) AS avg_read_at_quintile
    FROM combined_details_site
    GROUP BY site_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_read_at_quintile,
        ROUND(s.read_at_quintile - g.avg_read_at_quintile, 2) AS read_at_quintile_minus_avg
    FROM combined_details_site s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.read_at_quintile > g.avg_read_at_quintile
),
with_children AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.days_since_read_at,
        s.kwh_bucket,
        s.quality_sum_kwh_bucket,
        s.site_name,
        s.read_at_quintile_minus_avg,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage
    FROM outliers s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT site_id, reading_read_at, quality, reading_voltage
    FROM with_children
) src
PIVOT (
    MIN(reading_voltage) AS min_v, SUM(reading_voltage) AS sum_v
    FOR quality IN ('OK', 'EST')
)
