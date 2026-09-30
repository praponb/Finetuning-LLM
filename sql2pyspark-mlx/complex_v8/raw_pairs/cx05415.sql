WITH joined_ranges AS (
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
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
filtered AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM joined_ranges s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage <= 233.1
        )
      AND s.read_at >= TIMESTAMP '2025-01-19 07:00:00'
),
without_matches AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage >= 221.8
        )
      AND s.site_id IS NOT NULL
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    zone,
    rate,
    energy_cost,
    DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_label,
    COALESCE(kwh, 0.0) AS kwh_filled
FROM without_matches
WHERE read_at >= TIMESTAMP '2025-01-25 13:00:00'
ORDER BY kwh DESC NULLS FIRST, id
LIMIT 10
