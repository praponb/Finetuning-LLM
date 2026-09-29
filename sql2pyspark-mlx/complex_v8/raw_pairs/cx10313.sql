WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-03-08 10:00:00'
      AND site_id <= 7
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        read_at,
        kwh,
        NTH_VALUE(kwh, 2) OVER (PARTITION BY site_id ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id)) AS distinct_quality_in_site_id,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id ORDER BY voltage), 3) AS voltage_cume_dist,
        RANK() OVER (PARTITION BY quality ORDER BY read_at DESC) AS read_at_rank,
        SUM(voltage) OVER (PARTITION BY site_id ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_voltage,
        NTILE(4) OVER (PARTITION BY quality ORDER BY read_at DESC, id) AS read_at_quartile,
        MAX(voltage) OVER w_site_id AS site_id_max_voltage,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY site_id), 0), 4) AS voltage_share_of_site_id,
        voltage - LAG(voltage) OVER w_site_id_quality_by_read_at AS voltage_change,
        COUNT(*) OVER w_site_id AS rows_in_site_id,
        ROUND(kwh / NULLIF(SUM(kwh) OVER w_site_id, 0), 4) AS kwh_share_of_site_id,
        ROUND(AVG(voltage) OVER (PARTITION BY site_id ORDER BY voltage, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS voltage_avg_2_prior,
        MAX(kwh) OVER (PARTITION BY site_id ORDER BY voltage, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS kwh_max_1_prior,
        ROUND(AVG(kwh) OVER w_site_id_quality, 2) AS site_id_quality_avg_kwh,
        ROUND(PERCENT_RANK() OVER w_all_by_voltage_ties, 3) AS voltage_percent_rank,
        LAG(kwh, 1, 0) OVER w_all_by_voltage AS prev_kwh
    FROM base_readings
    WINDOW w_site_id AS (PARTITION BY site_id), w_site_id_quality_by_read_at AS (PARTITION BY site_id, quality ORDER BY read_at, id), w_site_id_quality AS (PARTITION BY site_id, quality), w_all_by_voltage_ties AS (ORDER BY voltage), w_all_by_voltage AS (ORDER BY voltage, id)
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        read_at,
        kwh,
        second_kwh,
        distinct_quality_in_site_id,
        voltage_cume_dist,
        running_voltage,
        read_at_quartile,
        site_id_max_voltage,
        voltage_change,
        rows_in_site_id,
        kwh_share_of_site_id,
        voltage_avg_2_prior,
        kwh_max_1_prior,
        site_id_quality_avg_kwh,
        voltage_percent_rank,
        prev_kwh,
        GREATEST(site_id_quality_avg_kwh, voltage_cume_dist) AS greatest_site_id_quality_avg_kwh_voltage_cume_di,
        TO_DATE(read_at) AS read_at_date,
        ABS(site_id_max_voltage - 994.0) AS site_id_max_voltage_distance
    FROM ranked
    WHERE voltage_cume_dist >= 0.4
)
-- Final projection
SELECT
    id,
    read_at,
    kwh,
    second_kwh,
    distinct_quality_in_site_id,
    voltage_cume_dist,
    running_voltage,
    read_at_quartile,
    site_id_max_voltage,
    kwh_share_of_site_id,
    voltage_avg_2_prior,
    kwh_max_1_prior,
    site_id_quality_avg_kwh,
    voltage_percent_rank,
    prev_kwh,
    greatest_site_id_quality_avg_kwh_voltage_cume_di,
    site_id_max_voltage_distance,
    CASE WHEN read_at_quartile >= 8 THEN 'large' WHEN read_at_quartile >= 3 THEN 'medium' ELSE 'small' END AS read_at_quartile_tier,
    ABS(site_id_max_voltage - 1778.0) AS site_id_max_voltage_distance_2
FROM enriched
WHERE site_id_max_voltage_distance BETWEEN 857.5 AND 1960.0
ORDER BY read_at DESC
