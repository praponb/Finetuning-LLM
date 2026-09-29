WITH matched AS (
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
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.zone,
        s.rate,
        s.energy_cost,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality
    FROM matched s
    JOIN meter_readings m ON m.site_id = s.site_id
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        reading_quality,
        MIN(reading_kwh) AS min_reading_kwh,
        COUNT(*) AS cnt
    FROM detail_rows_readings
    GROUP BY DATE_TRUNC('MONTH', read_at), reading_quality
)
-- Final projection
SELECT
    read_at_month,
    reading_quality,
    min_reading_kwh,
    DATE_FORMAT(read_at_month, 'yyyy-MM-dd HH:00') AS read_at_month_label
FROM per_group
ORDER BY min_reading_kwh DESC
