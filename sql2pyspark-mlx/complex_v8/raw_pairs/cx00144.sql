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
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
totals AS (
    -- Group-level totals
    SELECT
        zone,
        site_id,
        TO_DATE(read_at) AS read_at_day,
        SUM(energy_cost) AS total_energy_cost,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT id) AS distinct_ids
    FROM covered
    GROUP BY 1, 2, 3
    HAVING AVG(voltage) > 215.7
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM totals s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
            WHERE TO_DATE(m.read_at) < DATE '2025-02-06'
              AND m.quality NOT IN ('BAD', 'EST')
        )
      AND s.total_energy_cost < 4320.0
)
SELECT *
FROM (
    SELECT read_at_day, zone, total_energy_cost
    FROM screened
) src
PIVOT (
    SUM(total_energy_cost)
    FOR zone IN ('Z3', 'Z4')
)
ORDER BY read_at_day
