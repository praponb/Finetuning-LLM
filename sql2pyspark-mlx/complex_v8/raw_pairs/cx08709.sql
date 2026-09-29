WITH covered AS (
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
        s.read_at,
        s.kwh,
        s.voltage,
        s.zone,
        s.rate,
        s.energy_cost,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage
    FROM covered s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id AND TO_DATE(m.read_at) < DATE '2025-02-12'
),
screened AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM detail_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality NOT IN ('OK', 'EST')
        )
      AND s.kwh < 110.5
)
SELECT id, reading_id, measure, reading
FROM (
    SELECT id, reading_id, kwh, rate, energy_cost
    FROM screened
) src
UNPIVOT INCLUDE NULLS (
    reading FOR measure IN (kwh, rate, energy_cost)
) unp
ORDER BY id, reading_id, measure
