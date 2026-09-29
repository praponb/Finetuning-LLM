WITH in_window AS (
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
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(zone, 'UNKNOWN') AS zone_or_unknown,
        GREATEST(voltage, kwh) AS greatest_voltage_kwh
    FROM in_window
    WHERE site_id IS NOT NULL
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(CASE WHEN quality = 'BAD' THEN 1 END) AS reading_quality_count,
        COUNT(CASE WHEN quality IN ('OK', 'EST') THEN 1 END) AS reading_quality_count_2,
        COUNT(*) FILTER (WHERE quality IN ('OK', 'BAD')) AS reading_quality_count_3
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count,
        COALESCE(m.reading_quality_count_2, 0) AS reading_quality_count_2,
        COALESCE(m.reading_quality_count_3, 0) AS reading_quality_count_3
    FROM scored s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
derived AS (
    -- Label and score rows
    SELECT *, reading_quality_count_2 + rate AS reading_quality_count_2_plus_rate
    FROM with_counts
)
SELECT
    reading_quality_count_2,
    site_id,
    GROUPING(reading_quality_count_2) AS is_total_reading_quality_count_2,
    ROUND(SUM(voltage), 2) AS total_voltage,
    SUM(energy_cost) FILTER (WHERE zone = 'Z3') AS energy_cost_zone_z3,
    SUM(CASE WHEN zone IS NOT NULL THEN 1 ELSE 0 END) AS n_zone_is_not_null
FROM derived
GROUP BY ROLLUP(reading_quality_count_2, site_id)
