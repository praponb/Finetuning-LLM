WITH joined_ranges AS (
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
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        ROUND(AVG(voltage), 2) AS avg_reading_voltage,
        SUM(kwh) AS total_reading_kwh
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.avg_reading_voltage,
        m.total_reading_kwh
    FROM joined_ranges s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
SELECT
    id,
    read_at,
    kwh,
    quality,
    zone,
    rate,
    energy_cost,
    avg_reading_voltage,
    total_reading_kwh,
    ROUND(kwh * 0.9, 2) AS kwh_adj,
    COALESCE(avg_reading_voltage, 0.0) AS avg_reading_voltage_filled
FROM with_counts
ORDER BY quality DESC
