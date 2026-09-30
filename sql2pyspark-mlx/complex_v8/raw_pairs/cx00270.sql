WITH valid_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-01-19 13:00:00'
      AND kwh > 48.2
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(read_at) AS read_at_year,
        quality,
        site_id,
        MIN(voltage) AS min_voltage,
        ROUND(SUM(voltage) / NULLIF(SUM(kwh), 0), 3) AS voltage_to_kwh_ratio
    FROM valid_readings
    GROUP BY YEAR(read_at), quality, site_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        read_at_year,
        quality,
        site_id,
        min_voltage,
        voltage_to_kwh_ratio,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality)) AS distinct_site_id_in_quality,
        ROUND(AVG(read_at_year) OVER (PARTITION BY read_at_year ORDER BY voltage_to_kwh_ratio DESC, read_at_year, quality, site_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS read_at_year_avg_4,
        DENSE_RANK() OVER (PARTITION BY read_at_year, site_id ORDER BY voltage_to_kwh_ratio DESC NULLS FIRST, read_at_year, quality, site_id) AS voltage_to_kwh_ratio_dense_rank
    FROM summary
)
SELECT
    read_at_year,
    site_id,
    min_voltage,
    distinct_site_id_in_quality,
    read_at_year_avg_4,
    voltage_to_kwh_ratio_dense_rank,
    ROUND(voltage_to_kwh_ratio * 1.07, 2) AS voltage_to_kwh_ratio_adj,
    CASE WHEN quality IN ('EST', 'BAD') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group
FROM ranked
ORDER BY voltage_to_kwh_ratio_adj DESC
