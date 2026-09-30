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
summary AS (
    -- Group-level totals
    SELECT
        site_id,
        YEAR(read_at) AS read_at_year,
        COUNT(DISTINCT site_id) AS distinct_site_ids,
        MIN(kwh) AS min_kwh,
        SUM(CASE WHEN zone = 'Z3' THEN 1 ELSE 0 END) AS n_zone_z3,
        MIN(energy_cost) AS min_energy_cost,
        ROUND(AVG(voltage), 2) AS avg_voltage
    FROM in_window
    GROUP BY site_id, YEAR(read_at)
)
SELECT
    site_id,
    read_at_year,
    distinct_site_ids,
    min_kwh,
    n_zone_z3,
    min_energy_cost,
    avg_voltage
FROM summary
WHERE read_at_year <= 2026
ORDER BY site_id, n_zone_z3 DESC NULLS FIRST
