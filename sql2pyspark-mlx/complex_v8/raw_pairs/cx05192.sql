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
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
detail_rows AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.zone,
        s.energy_cost,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh
    FROM in_window s
    JOIN meter_readings m ON m.site_id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    voltage,
    zone,
    reading_id,
    reading_read_at,
    reading_voltage,
    reading_kwh,
    ROUND(reading_kwh / NULLIF(energy_cost, 0), 3) AS reading_kwh_per_energy_cost,
    COALESCE(reading_quality, 'UNKNOWN') AS reading_quality_or_unknown
FROM detail_rows
ORDER BY reading_quality_or_unknown DESC NULLS FIRST, zone NULLS LAST, id, reading_id
LIMIT 10
