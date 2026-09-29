WITH valid AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        ABS(voltage - 226.0) AS voltage_distance,
        QUARTER(read_at) AS read_at_quarter
    FROM meter_readings
    WHERE voltage < 234.7
      AND read_at >= TIMESTAMP '2025-02-06 12:00:00'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(site_id) OVER w_read_at_to_date_read_at_quar) AS distinct_site_id_in_read_at_to_date_read_at_quar,
        MAX(voltage) OVER w_read_at_to_date_read_at_quar AS read_at_to_date_read_at_quarter_max_voltage,
        RANK() OVER (PARTITION BY read_at_quarter ORDER BY read_at_quarter, id) AS read_at_quarter_rank,
        ROUND(read_at_quarter - AVG(read_at_quarter) OVER w_read_at_to_date_read_at_quar, 2) AS read_at_quarter_vs_read_at_to_date_read_at_quart,
        MIN(kwh) OVER (PARTITION BY TO_DATE(read_at), read_at_quarter) AS read_at_to_date_read_at_quarter_min_kwh
    FROM valid
    WINDOW w_read_at_to_date_read_at_quar AS (PARTITION BY TO_DATE(read_at), read_at_quarter)
),
with_activity AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM with_trends s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh < 116.8
        )
      AND s.distinct_site_id_in_read_at_to_date_read_at_quar >= 2
),
with_parent_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.voltage_distance,
        s.read_at_quarter,
        s.distinct_site_id_in_read_at_to_date_read_at_quar,
        s.read_at_to_date_read_at_quarter_max_voltage,
        s.read_at_quarter_rank,
        s.read_at_quarter_vs_read_at_to_date_read_at_quart,
        si.installed_kw AS site_installed_kw
    FROM with_activity s
    LEFT JOIN sites si ON si.id = s.site_id AND si.commissioned_on IS NOT NULL
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.distinct_site_id_in_read_at_to_date_read_at_quar,
        s.read_at_to_date_read_at_quarter_max_voltage,
        s.read_at_quarter_vs_read_at_to_date_read_at_quart,
        s.site_installed_kw,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality
    FROM with_parent_site s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id AND m.quality IS NOT NULL
)
-- Subtotals with GROUPING SETS
SELECT
    distinct_site_id_in_read_at_to_date_read_at_quar,
    reading_quality,
    COUNT(*) AS cnt,
    ROUND(STDDEV(reading_kwh), 2) AS stddev_reading_kwh,
    COUNT(DISTINCT site_id) AS distinct_site_ids
FROM detail_rows_readings
GROUP BY GROUPING SETS ((distinct_site_id_in_read_at_to_date_read_at_quar, reading_quality), (distinct_site_id_in_read_at_to_date_read_at_quar), (reading_quality))
ORDER BY distinct_site_id_in_read_at_to_date_read_at_quar NULLS LAST, reading_quality NULLS LAST
