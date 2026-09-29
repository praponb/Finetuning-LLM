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
detail_rows AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.zone,
        s.rate,
        s.energy_cost,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.kwh AS reading_kwh,
        m.quality AS reading_quality,
        m.voltage AS reading_voltage
    FROM matched s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
)
SELECT id, reading_id, metric, value
FROM (
    SELECT id, reading_id, reading_voltage, reading_kwh, energy_cost, rate
    FROM detail_rows
) src
UNPIVOT (
    value FOR metric IN (reading_voltage, reading_kwh, energy_cost, rate)
) unp
