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
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(m.voltage), 2) FROM meter_readings m WHERE m.site_id = s.site_id) AS avg_reading_voltage,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at
    FROM covered s
),
with_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.zone,
        s.avg_reading_voltage,
        s.first_reading_read_at,
        si.site_name,
        si.zone AS site_zone
    FROM with_stats s
    JOIN sites si ON si.id = s.site_id
)
SELECT
    id,
    site_id,
    kwh,
    zone,
    avg_reading_voltage,
    first_reading_read_at,
    site_name,
    CASE WHEN kwh >= 96.8 THEN 'HIGH' WHEN kwh >= 35.0 THEN 'MEDIUM' ELSE 'LOW' END AS kwh_tier,
    kwh - avg_reading_voltage AS kwh_minus_avg_reading_voltage
FROM with_details
WHERE HOUR(first_reading_read_at) BETWEEN 8 AND 12
