WITH cleaned AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on
    FROM sites
    WHERE UPPER(TRIM(site_name)) LIKE 'M%'
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(*) FILTER (WHERE quality IS NOT NULL) AS reading_quality_count,
        MAX(read_at) AS last_reading_read_at,
        ROUND(AVG(kwh), 2) AS avg_reading_kwh
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count,
        m.last_reading_read_at,
        m.avg_reading_kwh
    FROM cleaned s
    LEFT JOIN reading_stats m ON m.site_id = s.id
),
line_level_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.zone,
        s.tariff,
        s.installed_kw,
        s.commissioned_on,
        s.reading_quality_count,
        s.last_reading_read_at,
        s.avg_reading_kwh,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh
    FROM profiled s
    JOIN meter_readings m ON m.site_id = s.id
),
reading_stats_2 AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(CASE WHEN quality = 'OK' THEN 1 END) AS reading_quality_count_2
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_quality_count_2, 0) AS reading_quality_count_2
    FROM line_level_readings s
    LEFT JOIN reading_stats_2 m ON m.site_id = s.id
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.zone,
        s.tariff,
        s.installed_kw,
        s.commissioned_on,
        s.avg_reading_kwh,
        s.reading_id,
        s.reading_voltage,
        s.reading_kwh,
        s.reading_quality_count_2,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage_2,
        m.kwh AS reading_kwh_2
    FROM summarized_rows s
    JOIN meter_readings m ON m.id = s.reading_id AND m.read_at >= TIMESTAMP '2025-02-06 08:00:00'
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(tariff, 'ALL') AS tariff,
    reading_quality_count_2,
    GROUPING(tariff) AS is_total_tariff,
    COUNT(*) FILTER (WHERE zone = 'Z3') AS n_zone_z3
FROM combined_details
GROUP BY ROLLUP(tariff, reading_quality_count_2)
ORDER BY tariff NULLS LAST, reading_quality_count_2 NULLS LAST
