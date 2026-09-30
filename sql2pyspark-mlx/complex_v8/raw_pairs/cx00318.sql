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
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
qualified AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM matched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality NOT IN ('BAD', 'OK')
              AND m.kwh > 51.5
        )
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.voltage AS reading_voltage
    FROM qualified s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', read_at) AS days_since_read_at,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at_2,
        ROUND(reading_kwh / NULLIF(voltage, 0), 3) AS reading_kwh_per_voltage
    FROM detail_rows_readings
)
SELECT
    id,
    site_id,
    read_at,
    voltage,
    reading_kwh,
    reading_voltage,
    days_since_read_at,
    reading_kwh_per_voltage,
    DAYOFWEEK(read_at) AS read_at_dayofweek,
    ABS(reading_voltage - 224.9) AS reading_voltage_distance
FROM scored
WHERE days_since_read_at_2 BETWEEN 210 AND 595
ORDER BY id, site_id
