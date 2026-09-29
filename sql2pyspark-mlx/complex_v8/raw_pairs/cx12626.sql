WITH recent_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE site_id >= 6
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(voltage * 0.25, 2) AS voltage_adj,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        ROUND(voltage * 1.1, 2) AS voltage_adj_2
    FROM recent_readings
    WHERE quality <> 'BAD'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(voltage) OVER (ORDER BY kwh, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS voltage_sum_3,
        LAG(kwh) OVER (ORDER BY kwh, id) AS prev_kwh,
        NTH_VALUE(kwh, 2) OVER (ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        RANK() OVER (ORDER BY kwh) AS kwh_rank,
        MAX(voltage) OVER (ORDER BY voltage DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS voltage_max_2_prior
    FROM tagged
)
SELECT
    id,
    site_id,
    kwh,
    quality,
    voltage_adj,
    quality_slug,
    voltage_sum_3,
    prev_kwh,
    kwh_rank
FROM metrics
WHERE kwh BETWEEN 16.5 AND 82.9
ORDER BY kwh_rank DESC
