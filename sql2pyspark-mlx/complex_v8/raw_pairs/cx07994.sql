WITH covered AS (
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
without_matches AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM covered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.read_at >= TIMESTAMP '2025-02-06 12:00:00'
        )
),
screened AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM without_matches s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage BETWEEN 213.2 AND 240.1
        )
)
SELECT id, field, value
FROM (
    SELECT id, kwh, rate, energy_cost
    FROM screened
) src
UNPIVOT INCLUDE NULLS (
    value FOR field IN (kwh AS kwh_v, rate AS m_rate, energy_cost AS m_energy_cost)
) unp
