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
with_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.rate,
        s.energy_cost,
        si.commissioned_on AS site_commissioned_on
    FROM in_window s
    LEFT JOIN sites si ON si.id = s.site_id AND si.commissioned_on >= DATE '2024-06-27'
),
with_activity AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM with_details_site s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage >= 216.1
              AND m.kwh BETWEEN 35.0 AND 106.0
        )
      AND s.rate <= 7.6
)
SELECT
    id,
    site_id,
    kwh,
    rate,
    site_commissioned_on
FROM with_activity
ORDER BY rate, site_commissioned_on DESC
