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
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
line_level AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.energy_cost,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at
    FROM matched s
    JOIN meter_readings m ON m.site_id = s.site_id
),
with_details AS (
    -- Attach reading attributes
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.energy_cost,
        s.reading_id,
        s.reading_voltage,
        s.reading_quality,
        s.reading_kwh,
        s.reading_read_at,
        m.kwh AS reading_kwh_2
    FROM line_level s
    LEFT JOIN meter_readings m ON m.id = s.reading_id
)
SELECT
    COALESCE(reading_quality, 'ALL') AS reading_quality,
    site_id,
    reading_id,
    GROUPING(reading_quality) AS is_total_reading_quality,
    COUNT(DISTINCT reading_quality) AS distinct_reading_qualitys,
    MAX(energy_cost) AS max_energy_cost
FROM with_details
GROUP BY CUBE(reading_quality, site_id, reading_id)
