WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LENGTH(TRIM(quality)) AS quality_len,
        CASE WHEN voltage >= 232.4 THEN 'A' WHEN voltage >= 219.6 THEN 'B' ELSE 'C' END AS voltage_tier
    FROM meter_readings
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN kwh >= 110.5 THEN 'large' WHEN kwh >= 30.2 THEN 'medium' ELSE 'small' END AS kwh_tier
    FROM prepared
    WHERE site_id IS NOT NULL
),
latest AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        quality_len,
        voltage_tier,
        kwh_tier
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY read_at DESC, id DESC) AS recency_rank
        FROM tagged
    ) ranked
    WHERE recency_rank = 1
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(voltage) OVER (ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_voltage,
        SUM(kwh) OVER (PARTITION BY quality) AS quality_sum_kwh,
        MIN(voltage) OVER (ORDER BY voltage, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_voltage_so_far,
        FIRST_VALUE(kwh) OVER (PARTITION BY site_id ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh
    FROM latest
)
-- Subtotals with GROUPING SETS
SELECT
    site_id,
    quality_len,
    voltage_tier,
    COUNT(DISTINCT site_id) AS distinct_site_ids,
    COUNT(*) AS row_count,
    PERCENTILE_APPROX(last_voltage, 0.5) AS median_last_voltage
FROM metrics
GROUP BY GROUPING SETS ((site_id, quality_len, voltage_tier), (site_id), (quality_len))
ORDER BY site_id NULLS LAST, quality_len NULLS LAST, voltage_tier NULLS LAST
