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
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN zone IN ('Z4', 'Z3') THEN 'group_a' WHEN zone = 'Z2' THEN 'group_b' ELSE 'other' END AS zone_group,
        ROUND(voltage / NULLIF(rate, 0), 3) AS voltage_per_rate
    FROM joined_ranges
    WHERE kwh <= 113.0
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        zone,
        rate,
        energy_cost,
        zone_group,
        voltage_per_rate,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY zone_group)) AS distinct_site_id_in_zone_group,
        MIN(rate) OVER (PARTITION BY zone_group) AS zone_group_min_rate
    FROM labeled
),
unique_rows AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        zone,
        rate,
        energy_cost,
        zone_group,
        voltage_per_rate,
        distinct_site_id_in_zone_group,
        zone_group_min_rate
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY kwh DESC, id DESC) AS rn
        FROM with_windows
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    rate,
    energy_cost,
    voltage_per_rate,
    distinct_site_id_in_zone_group,
    zone_group_min_rate,
    ABS(voltage - 233.2) AS voltage_distance,
    DAYOFWEEK(read_at) AS read_at_dayofweek
FROM unique_rows
ORDER BY voltage DESC
