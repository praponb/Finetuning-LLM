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
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(voltage - 226.0) AS voltage_distance,
        FLOOR(energy_cost / 250) * 250 AS energy_cost_bucket
    FROM joined_ranges
    WHERE rate > 3.0
),
filtered AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage BETWEEN 219.6 AND 232.0
              AND m.kwh <= 84.9
        )
      AND s.energy_cost >= 180.0
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.zone,
        s.energy_cost,
        s.voltage_distance,
        s.energy_cost_bucket,
        si.site_name
    FROM filtered s
    JOIN sites si ON si.id = s.site_id
)
SELECT
    id,
    site_id,
    read_at,
    zone,
    energy_cost,
    voltage_distance,
    energy_cost_bucket,
    site_name,
    COALESCE(zone, 'UNKNOWN') AS zone_or_unknown,
    voltage_distance - energy_cost AS voltage_distance_minus_energy_cost
FROM with_parent
WHERE voltage_distance >= 73.5
ORDER BY read_at DESC
