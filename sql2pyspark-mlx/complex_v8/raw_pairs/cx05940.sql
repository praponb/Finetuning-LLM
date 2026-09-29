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
with_activity AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM covered s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality IN ('EST', 'OK')
              AND m.kwh >= 52.5
        )
      AND s.kwh <= 109.0
),
latest AS (
    -- Keep the earliest row per zone
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        zone,
        rate,
        energy_cost
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY zone ORDER BY kwh, id) AS row_num
        FROM with_activity
    ) ranked
    WHERE row_num = 1
),
filtered AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality IS NOT NULL
              AND m.read_at < TIMESTAMP '2025-02-18 12:00:00'
        )
)
SELECT
    id,
    site_id,
    kwh,
    voltage,
    zone,
    rate,
    energy_cost,
    CASE WHEN zone IN ('Z3', 'Z4') THEN 'group_a' WHEN zone = 'Z2' THEN 'group_b' ELSE 'other' END AS zone_group,
    COALESCE(zone, 'UNKNOWN') AS zone_or_unknown
FROM filtered
