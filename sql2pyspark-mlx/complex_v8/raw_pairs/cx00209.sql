WITH covered AS (
    -- Price each reading with the tariff window it falls into
    SELECT
        r.id,
        r.site_id,
        r.read_at,
        r.kwh,
        r.quality,
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
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MIN(voltage) AS min_reading_voltage
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.min_reading_voltage
    FROM covered s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
-- Final projection
SELECT DISTINCT quality, id
FROM summarized_rows
ORDER BY quality NULLS LAST, id DESC
