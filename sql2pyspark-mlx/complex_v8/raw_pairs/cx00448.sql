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
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM in_window s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality = 'BAD'
        )
      AND s.kwh BETWEEN 33.9 AND 84.9
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        zone,
        TO_DATE(read_at) AS read_at_day,
        site_id,
        ROUND(STDDEV(voltage), 2) AS stddev_voltage,
        COUNT(*) AS n_rows,
        ROUND(SUM(rate) / NULLIF(SUM(energy_cost), 0), 3) AS rate_to_energy_cost_ratio
    FROM without_matches
    WHERE rate >= 3.7
    GROUP BY zone, TO_DATE(read_at), site_id
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE rate_to_energy_cost_ratio > (SELECT AVG(rate_to_energy_cost_ratio) FROM stats)
)
-- Final projection
SELECT
    zone,
    read_at_day,
    site_id,
    stddev_voltage,
    n_rows,
    rate_to_energy_cost_ratio,
    SUBSTRING(TRIM(zone), 1, 2) AS zone_prefix
FROM with_activity
WHERE stddev_voltage BETWEEN 24.5 AND 183.8
ORDER BY rate_to_energy_cost_ratio DESC
