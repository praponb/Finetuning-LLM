WITH base_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        GREATEST(voltage, kwh) AS greatest_voltage_kwh,
        CASE WHEN voltage >= 232.0 THEN 'HIGH' WHEN voltage >= 222.5 THEN 'MEDIUM' ELSE 'LOW' END AS voltage_tier
    FROM meter_readings
    WHERE (read_at >= TIMESTAMP '2025-01-19 13:00:00' OR site_id <= 7)
),
latest_per_key AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        greatest_voltage_kwh,
        voltage_tier
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY greatest_voltage_kwh DESC, id DESC) AS row_num
        FROM base_readings
    ) ranked
    WHERE row_num = 1
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id, voltage_tier ORDER BY kwh), 3) AS kwh_cume_dist,
        ROW_NUMBER() OVER (ORDER BY voltage DESC, id) AS rn,
        SUM(greatest_voltage_kwh) OVER (PARTITION BY site_id) AS site_id_sum_greatest_voltage_kwh,
        ROUND(AVG(kwh) OVER (PARTITION BY site_id, voltage_tier ORDER BY kwh, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_7,
        ROUND(AVG(voltage) OVER (ORDER BY voltage DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS voltage_avg_7,
        DENSE_RANK() OVER w_all_by_voltage_ties AS voltage_dense_rank
    FROM latest_per_key
    WINDOW w_all_by_voltage_ties AS (ORDER BY voltage DESC)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN voltage_tier IN ('HIGH', 'LOW') THEN 'group_a' WHEN voltage_tier = 'MEDIUM' THEN 'group_b' ELSE 'other' END AS voltage_tier_group
    FROM metrics
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    voltage,
    quality,
    greatest_voltage_kwh,
    voltage_tier,
    rn,
    kwh_avg_7,
    voltage_avg_7,
    voltage_dense_rank,
    LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
FROM derived
ORDER BY quality_slug DESC, voltage_dense_rank DESC
