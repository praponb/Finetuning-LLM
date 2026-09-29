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
filtered AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM matched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality = 'OK'
        )
      AND s.kwh < 101.2
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    zone,
    rate,
    energy_cost
FROM filtered
WHERE rate > 3.0
ORDER BY site_id DESC, zone NULLS LAST
