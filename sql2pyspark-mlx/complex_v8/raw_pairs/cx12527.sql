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
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
without_matches AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM in_window s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh >= 14.5
        )
      AND s.kwh < 110.5
)
SELECT
    id,
    site_id,
    kwh,
    voltage,
    zone,
    rate,
    energy_cost,
    DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at
FROM without_matches
ORDER BY voltage
