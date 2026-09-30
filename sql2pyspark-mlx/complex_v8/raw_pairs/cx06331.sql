WITH scoped_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        INITCAP(TRIM(quality)) AS quality_clean,
        GREATEST(voltage, kwh) AS greatest_voltage_kwh,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND voltage BETWEEN 220.2 AND 239.6
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        COALESCE(voltage, 0.0) AS voltage_filled,
        ROUND(kwh * 0.25, 2) AS kwh_adj,
        SUBSTRING(TRIM(quality_clean), 1, 4) AS quality_clean_prefix
    FROM scoped_readings
    WHERE kwh <= 104.8
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM derived
    WHERE voltage_filled < (SELECT MAX(voltage_filled) FROM derived)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(voltage_filled) OVER (ORDER BY voltage_filled NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS voltage_filled_max_2_prior,
        FIRST_VALUE(voltage_filled) IGNORE NULLS OVER (ORDER BY kwh_adj, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_voltage_filled
    FROM screened
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY voltage_filled_max_2_prior NULLS LAST) AS voltage_filled_max_2_prior_dense_rank,
        SUM(first_known_voltage_filled) OVER (ORDER BY voltage_filled_max_2_prior NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS first_known_voltage_filled_sum_3,
        SUM(kwh_adj) OVER (ORDER BY voltage_filled_max_2_prior NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh_adj,
        FIRST_VALUE(voltage_filled) IGNORE NULLS OVER (ORDER BY voltage_filled_max_2_prior NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_voltage_filled_2
    FROM ranked
)
-- Final projection
SELECT
    id,
    voltage_filled,
    quality_clean_prefix,
    voltage_filled_max_2_prior,
    voltage_filled_max_2_prior_dense_rank,
    first_known_voltage_filled_sum_3,
    first_known_voltage_filled_2
FROM metrics
ORDER BY voltage_filled_max_2_prior NULLS LAST
