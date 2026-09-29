WITH recent_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ABS(voltage - 223.1) AS voltage_distance
    FROM meter_readings
    WHERE (kwh > 48.2 OR quality IN ('EST', 'BAD'))
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.voltage_distance,
        si.site_name,
        si.tariff AS site_tariff,
        si.zone AS site_zone
    FROM recent_readings s
    JOIN sites si ON si.id = s.site_id AND si.tariff = 'EV'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        voltage_distance,
        site_name,
        site_zone,
        SIZE(COLLECT_SET(site_tariff) OVER (PARTITION BY site_zone)) AS distinct_site_tariff_in_site_zone,
        LAG(voltage) OVER (ORDER BY voltage_distance, id) AS prev_voltage,
        ROUND(CUME_DIST() OVER (PARTITION BY site_tariff, site_zone ORDER BY voltage DESC), 3) AS voltage_cume_dist,
        MAX(voltage) OVER (PARTITION BY site_zone) AS site_zone_max_voltage,
        COUNT(*) OVER w_site_id_site_tariff AS rows_in_site_id_site_tariff,
        MAX(voltage_distance) OVER (PARTITION BY site_zone) AS site_zone_max_voltage_distance,
        LAST_VALUE(voltage_distance) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage_distance,
        MAX(voltage) OVER (ORDER BY voltage_distance, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS voltage_max_3,
        ROUND(AVG(voltage_distance) OVER (PARTITION BY site_id, site_tariff), 2) AS site_id_site_tariff_avg_voltage_distance,
        SUM(voltage) OVER (PARTITION BY site_id ORDER BY read_at DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS voltage_sum_4
    FROM combined_details
    WINDOW w_site_id_site_tariff AS (PARTITION BY site_id, site_tariff)
),
annotated AS (
    -- Add calculated columns
    SELECT *, DATE_FORMAT(read_at, 'yyyy-MM-dd') AS read_at_label
    FROM sequenced
    WHERE rows_in_site_id_site_tariff <= 16
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        voltage,
        voltage_distance,
        site_name,
        site_zone,
        distinct_site_tariff_in_site_zone,
        prev_voltage,
        voltage_cume_dist,
        site_zone_max_voltage_distance,
        last_known_voltage_distance,
        voltage_max_3,
        site_id_site_tariff_avg_voltage_distance,
        voltage_sum_4,
        read_at_label,
        ROUND(site_zone_max_voltage - AVG(site_zone_max_voltage) OVER (PARTITION BY distinct_site_tariff_in_site_zone), 2) AS site_zone_max_voltage_vs_distinct_site_tariff_in,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY site_zone, distinct_site_tariff_in_site_zone), 0), 4) AS voltage_share_of_site_zone_distinct_site_tariff_,
        MIN(last_known_voltage_distance) OVER (PARTITION BY quality, site_id ORDER BY last_known_voltage_distance DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_last_known_voltage_distance_so_far,
        ROUND(voltage - AVG(voltage) OVER w_distinct_site_tariff_in_site, 2) AS voltage_vs_distinct_site_tariff_in_site_zone_avg
    FROM annotated
    WINDOW w_distinct_site_tariff_in_site AS (PARTITION BY distinct_site_tariff_in_site_zone)
)
SELECT DISTINCT site_id_site_tariff_avg_voltage_distance
FROM with_windows
WHERE read_at_label IS NOT NULL
ORDER BY site_id_site_tariff_avg_voltage_distance DESC
