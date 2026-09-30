WITH prepared_readings AS (
    -- Filter meter_readings
    SELECT *
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-02-18 07:00:00'
      AND quality IS NOT NULL
),
combined_details AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on
    FROM prepared_readings s
    JOIN sites si ON si.id = s.site_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        site_zone,
        NTILE(5) OVER w_site_id_site_zone_by_site_co AS site_commissioned_on_quintile,
        MAX(kwh) OVER w_site_id AS site_id_max_kwh,
        SIZE(COLLECT_SET(site_zone) OVER w_site_id) AS distinct_site_zone_in_site_id,
        voltage - LAG(voltage) OVER w_site_zone_by_read_at AS voltage_change,
        SUM(voltage) OVER (PARTITION BY site_id) AS site_id_sum_voltage,
        FIRST_VALUE(read_at) IGNORE NULLS OVER (PARTITION BY site_zone, TO_DATE(read_at) ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_read_at,
        DENSE_RANK() OVER (PARTITION BY site_zone, TO_DATE(read_at) ORDER BY read_at) AS read_at_dense_rank,
        NTILE(5) OVER (PARTITION BY site_zone, TO_DATE(read_at) ORDER BY read_at, id) AS read_at_quintile
    FROM combined_details
    WINDOW w_site_id_site_zone_by_site_co AS (PARTITION BY site_id, site_zone ORDER BY site_commissioned_on ASC, id), w_site_id AS (PARTITION BY site_id), w_site_zone_by_read_at AS (PARTITION BY site_zone ORDER BY read_at, id)
),
per_group AS (
    -- Group-level totals
    SELECT
        site_zone,
        distinct_site_zone_in_site_id,
        read_at_dense_rank,
        SUM(CASE WHEN site_zone IN ('Z4', 'Z3') THEN distinct_site_zone_in_site_id ELSE 0 END) AS distinct_site_zone_in_site_id_site_zone_in_z4_z3,
        ROUND(STDDEV(distinct_site_zone_in_site_id), 2) AS stddev_distinct_site_zone_in_site_id,
        COUNT(*) AS row_count,
        COUNT(DISTINCT site_zone) AS distinct_site_zones,
        COUNT(DISTINCT site_id) AS distinct_site_ids
    FROM with_windows
    GROUP BY 1, 2, 3
)
-- Final projection
SELECT
    site_zone,
    distinct_site_zone_in_site_id,
    read_at_dense_rank,
    distinct_site_zone_in_site_id_site_zone_in_z4_z3,
    stddev_distinct_site_zone_in_site_id,
    row_count,
    distinct_site_zones,
    distinct_site_ids,
    CASE WHEN distinct_site_zones >= 7 THEN 'A' WHEN distinct_site_zones >= 4 THEN 'B' ELSE 'C' END AS distinct_site_zones_tier,
    INITCAP(TRIM(site_zone)) AS site_zone_clean
FROM per_group
WHERE distinct_site_zone_in_site_id >= 4
