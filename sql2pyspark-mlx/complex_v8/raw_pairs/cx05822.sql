WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        kwh - voltage AS kwh_minus_voltage,
        ABS(voltage - 222.5) AS voltage_distance
    FROM meter_readings
    WHERE quality IN ('OK', 'BAD')
),
joined AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        s.kwh_minus_voltage,
        s.voltage_distance,
        si.zone AS site_zone,
        si.site_name
    FROM base_readings s
    JOIN sites si ON si.id = s.site_id AND si.commissioned_on >= DATE '2024-08-24'
    WHERE si.zone IN ('Z4', 'Z2')
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        read_at,
        voltage_distance,
        site_name,
        ROUND(AVG(voltage_distance) OVER (PARTITION BY site_zone, YEAR(read_at)), 2) AS site_zone_read_at_year_avg_voltage_distance,
        LAG(kwh_minus_voltage, 3) OVER w_all_by_kwh_minus_voltage AS prev3_kwh_minus_voltage,
        NTILE(10) OVER (PARTITION BY site_id ORDER BY voltage_distance ASC, id) AS voltage_distance_decile,
        NTILE(3) OVER w_site_id_quality_by_voltage_d AS voltage_distance_tercile,
        MAX(kwh_minus_voltage) OVER (PARTITION BY site_zone) AS site_zone_max_kwh_minus_voltage,
        NTILE(5) OVER (ORDER BY kwh_minus_voltage, id) AS kwh_minus_voltage_quintile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY site_id, site_zone ORDER BY kwh_minus_voltage DESC, id), 3) AS kwh_minus_voltage_percent_rank,
        MAX(voltage_distance) OVER (PARTITION BY site_id, quality ORDER BY voltage_distance DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS voltage_distance_max_2_prior,
        ROUND(kwh_minus_voltage / NULLIF(SUM(kwh_minus_voltage) OVER w_site_zone, 0), 4) AS kwh_minus_voltage_share_of_site_zone,
        ROUND(voltage_distance - AVG(voltage_distance) OVER (PARTITION BY site_zone, YEAR(read_at)), 2) AS voltage_distance_vs_site_zone_read_at_year_avg,
        MIN(kwh_minus_voltage) OVER (PARTITION BY site_id, quality) AS site_id_quality_min_kwh_minus_voltage,
        ROW_NUMBER() OVER (ORDER BY kwh_minus_voltage DESC, id) AS seq_no
    FROM joined
    WINDOW w_all_by_kwh_minus_voltage AS (ORDER BY kwh_minus_voltage DESC, id), w_site_id_quality_by_voltage_d AS (PARTITION BY site_id, quality ORDER BY voltage_distance DESC, id), w_site_zone AS (PARTITION BY site_zone)
),
top_rows AS (
    -- Top 5 per group
    SELECT
        id,
        read_at,
        voltage_distance,
        site_name,
        site_zone_read_at_year_avg_voltage_distance,
        prev3_kwh_minus_voltage,
        voltage_distance_decile,
        voltage_distance_tercile,
        site_zone_max_kwh_minus_voltage,
        kwh_minus_voltage_quintile,
        kwh_minus_voltage_percent_rank,
        voltage_distance_max_2_prior,
        kwh_minus_voltage_share_of_site_zone,
        voltage_distance_vs_site_zone_read_at_year_avg,
        site_id_quality_min_kwh_minus_voltage
    FROM windowed
    WHERE seq_no <= 5
)
-- Final projection
SELECT
    id,
    read_at,
    site_name,
    site_zone_read_at_year_avg_voltage_distance,
    voltage_distance_tercile,
    site_zone_max_kwh_minus_voltage,
    kwh_minus_voltage_quintile,
    voltage_distance_max_2_prior,
    kwh_minus_voltage_share_of_site_zone,
    CASE WHEN voltage_distance >= 183.8 THEN 'HIGH' WHEN voltage_distance >= 61.2 THEN 'MEDIUM' ELSE 'LOW' END AS voltage_distance_tier
FROM top_rows
