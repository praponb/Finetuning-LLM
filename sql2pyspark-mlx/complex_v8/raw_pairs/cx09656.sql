WITH in_window AS (
    -- Price each reading with the tariff window it falls into
    SELECT
        r.id,
        r.site_id,
        r.read_at,
        r.kwh,
        r.quality,
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
with_activity AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM in_window s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
      AND s.zone IN ('Z4', 'Z3')
)
-- Final projection
SELECT
    id,
    read_at,
    quality,
    zone,
    rate,
    energy_cost,
    DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label
FROM with_activity
