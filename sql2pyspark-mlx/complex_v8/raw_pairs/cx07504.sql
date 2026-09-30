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
exploded_detail_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.rate,
        s.energy_cost,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh
    FROM in_window s
    JOIN meter_readings m ON m.site_id = s.site_id AND m.kwh <= 79.7
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        voltage,
        rate,
        energy_cost,
        reading_id,
        reading_kwh,
        ROUND(kwh / NULLIF(reading_voltage, 0), 3) AS kwh_per_reading_voltage
    FROM exploded_detail_readings
)
SELECT
    id,
    site_id,
    voltage,
    energy_cost,
    reading_kwh
FROM labeled
ORDER BY voltage DESC, id NULLS LAST
