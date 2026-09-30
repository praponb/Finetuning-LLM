WITH filtered_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        UPPER(TRIM(quality)) AS quality_clean,
        COALESCE(kwh, 0.0) AS kwh_filled
    FROM meter_readings
    WHERE (voltage > 223.3 OR kwh >= 35.0)
),
site_id_stats AS (
    -- Max voltage per site_id
    SELECT site_id, MAX(voltage) AS max_voltage
    FROM filtered_readings
    GROUP BY site_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_voltage,
        ROUND(s.voltage - g.max_voltage, 2) AS voltage_minus_max
    FROM filtered_readings s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.voltage = g.max_voltage
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        s.quality_clean,
        s.kwh_filled,
        s.max_voltage,
        si.zone AS site_zone,
        si.tariff AS site_tariff
    FROM vs_group s
    JOIN sites si ON si.id = s.site_id
    WHERE si.zone <> 'Z4'
),
detail_rows AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        s.quality_clean,
        s.max_voltage,
        s.site_zone,
        s.site_tariff,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality
    FROM with_details s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id AND m.quality IS NOT NULL
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(max_voltage / NULLIF(SUM(max_voltage) OVER (PARTITION BY site_zone, reading_quality), 0), 4) AS max_voltage_share_of_site_zone_reading_quality,
        LEAD(reading_read_at, 2) OVER w_quality_by_reading_kwh AS next2_reading_read_at,
        SUM(max_voltage) OVER w_site_zone_reading_quality AS site_zone_reading_quality_sum_max_voltage,
        RANK() OVER w_quality_by_reading_kwh_ties AS reading_kwh_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY site_zone ORDER BY max_voltage DESC, id, reading_id), 3) AS max_voltage_cume_dist
    FROM detail_rows
    WINDOW w_quality_by_reading_kwh AS (PARTITION BY quality ORDER BY reading_kwh NULLS LAST, id, reading_id), w_site_zone_reading_quality AS (PARTITION BY site_zone, reading_quality), w_quality_by_reading_kwh_ties AS (PARTITION BY quality ORDER BY reading_kwh NULLS LAST)
)
SELECT
    reading_kwh_rank,
    site_id,
    GROUPING_ID() AS grouping_level,
    MIN(next2_reading_read_at) AS first_next2_reading_read_at
FROM with_windows
GROUP BY GROUPING SETS ((reading_kwh_rank, site_id), (reading_kwh_rank), (site_id))
ORDER BY reading_kwh_rank NULLS LAST, site_id NULLS LAST
