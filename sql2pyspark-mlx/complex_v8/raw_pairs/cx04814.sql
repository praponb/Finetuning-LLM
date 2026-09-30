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
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
with_children_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        s.zone,
        s.energy_cost,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.voltage AS reading_voltage,
        m.read_at AS reading_read_at
    FROM covered s
    JOIN meter_readings m ON m.site_id = s.site_id
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality,
    zone,
    reading_id,
    reading_voltage
FROM with_children_readings
WHERE quality = 'OK'
ORDER BY kwh DESC
