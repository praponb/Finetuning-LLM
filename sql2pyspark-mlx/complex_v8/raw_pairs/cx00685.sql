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
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
enriched_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.zone,
        s.rate,
        si.tariff AS site_tariff,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on
    FROM joined_ranges s
    JOIN sites si ON si.id = s.site_id
    WHERE si.zone IS NOT NULL
),
without_matches AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM enriched_site s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality = 'BAD'
              AND MONTH(m.read_at) IN (2, 3)
        )
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    zone,
    site_commissioned_on
FROM without_matches
ORDER BY site_id DESC NULLS FIRST
