WITH joined_ranges AS (
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
combined_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.zone,
        si.tariff AS site_tariff
    FROM joined_ranges s
    LEFT JOIN sites si ON si.id = s.site_id
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.zone,
        s.site_tariff,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh
    FROM combined_details s
    JOIN meter_readings m ON m.site_id = s.site_id AND m.read_at < TIMESTAMP '2025-03-08 10:00:00'
),
screened AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM detail_rows_readings s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh > 52.5
        )
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT reading_quality, zone, voltage
    FROM screened
) src
PIVOT (
    SUM(voltage)
    FOR zone IN ('Z4', 'Z2', 'Z3')
)
ORDER BY reading_quality
