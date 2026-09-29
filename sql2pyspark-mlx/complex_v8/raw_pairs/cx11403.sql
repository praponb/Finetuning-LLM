WITH joined_ranges AS (
    -- Price each reading with the tariff window it falls into
    SELECT
        r.id,
        r.site_id,
        r.read_at,
        r.kwh,
        r.voltage,
        st.zone,
        tw.rate,
        ROUND(r.kwh * tw.rate, 2) AS energy_cost
    FROM meter_readings r
    JOIN sites st ON st.id = r.site_id
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        site_id,
        ARRAY_SORT(COLLECT_SET(zone)) AS zone_list,
        ROUND(SUM(voltage), 2) AS total_voltage,
        COUNT(DISTINCT zone) AS distinct_zones
    FROM joined_ranges
    GROUP BY DATE_TRUNC('MONTH', read_at), site_id
),
line_level AS (
    -- One row per reading
    SELECT
        s.read_at_month,
        s.site_id,
        s.zone_list,
        s.total_voltage,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage
    FROM stats s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', reading_read_at), reading_id ORDER BY reading_read_at ASC) AS reading_read_at_rank,
        SIZE(COLLECT_SET(reading_id) OVER w_reading_quality_read_at_mont) AS distinct_reading_id_in_reading_quality_read_at_m,
        MIN(total_voltage) OVER (PARTITION BY reading_quality, YEAR(read_at_month)) AS reading_quality_read_at_month_year_min_total_vol
    FROM line_level
    WINDOW w_reading_quality_read_at_mont AS (PARTITION BY reading_quality, YEAR(read_at_month))
)
-- Native SQL UNPIVOT (wide to long)
SELECT read_at_month, site_id, metric_name, value
FROM (
    SELECT read_at_month, site_id, reading_quality_read_at_month_year_min_total_vol, total_voltage, reading_voltage
    FROM windowed
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric_name IN (reading_quality_read_at_month_year_min_total_vol, total_voltage, reading_voltage)
) unp
