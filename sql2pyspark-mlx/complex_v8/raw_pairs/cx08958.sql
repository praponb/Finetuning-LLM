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
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        read_at,
        kwh,
        zone,
        energy_cost,
        COALESCE(kwh, 0.0) AS kwh_filled,
        CASE WHEN zone IN ('Z4', 'Z3') THEN 'group_a' WHEN zone = 'Z2' THEN 'group_b' ELSE 'other' END AS zone_group
    FROM joined_ranges
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        read_at,
        kwh,
        energy_cost,
        kwh_filled,
        LEAD(energy_cost, 1, 0) OVER (PARTITION BY zone, zone_group ORDER BY read_at, id) AS next_energy_cost,
        energy_cost - LAG(energy_cost) OVER (PARTITION BY zone, zone_group ORDER BY read_at, id) AS energy_cost_change
    FROM enriched
)
-- Final projection
SELECT
    id,
    read_at,
    energy_cost,
    kwh_filled,
    energy_cost_change,
    ABS(next_energy_cost - 315.0) AS next_energy_cost_distance
FROM with_trends
WHERE kwh >= 48.2
ORDER BY energy_cost_change, next_energy_cost_distance
