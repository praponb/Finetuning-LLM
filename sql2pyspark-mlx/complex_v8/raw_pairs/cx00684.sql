WITH recent_readings AS (
    -- Filter meter_readings
    SELECT *
    FROM meter_readings
    WHERE quality <> 'EST'
      AND kwh <= 106.5
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        read_at,
        kwh,
        voltage,
        quality,
        MAX(kwh) OVER (PARTITION BY quality) AS quality_max_kwh,
        SUM(kwh) OVER (ORDER BY kwh, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS kwh_sum_3_prior,
        LAST_VALUE(voltage) IGNORE NULLS OVER (ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage,
        NTILE(10) OVER (ORDER BY kwh, id) AS kwh_decile,
        ROUND(voltage - AVG(voltage) OVER (PARTITION BY quality), 2) AS voltage_vs_quality_avg
    FROM recent_readings
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM windowed
    WHERE last_known_voltage > (SELECT AVG(last_known_voltage) FROM windowed)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(read_at, 3) OVER (PARTITION BY quality ORDER BY last_known_voltage DESC, id) AS next3_read_at,
        LAG(kwh, 1, 0) OVER (PARTITION BY quality ORDER BY last_known_voltage DESC, id) AS prev_kwh,
        ROUND(AVG(quality_max_kwh) OVER (PARTITION BY kwh_decile, quality), 2) AS kwh_decile_quality_avg_quality_max_kwh
    FROM screened
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        read_at,
        kwh,
        voltage,
        quality,
        kwh_sum_3_prior,
        kwh_decile,
        voltage_vs_quality_avg,
        next3_read_at,
        kwh_decile_quality_avg_quality_max_kwh,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        TO_DATE(read_at) AS read_at_date,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix
    FROM metrics
    WHERE next3_read_at >= TIMESTAMP '2025-01-13 15:00:00'
),
windowed_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        kwh,
        voltage,
        quality,
        kwh_sum_3_prior,
        voltage_vs_quality_avg,
        kwh_decile_quality_avg_quality_max_kwh,
        quality_slug,
        read_at_date,
        quality_prefix,
        MIN(voltage_vs_quality_avg) OVER (PARTITION BY kwh_decile, quality) AS kwh_decile_quality_min_voltage_vs_quality_avg,
        DENSE_RANK() OVER (PARTITION BY quality ORDER BY read_at ASC) AS read_at_dense_rank
    FROM derived
)
-- Final projection
SELECT
    id,
    voltage,
    quality,
    kwh_sum_3_prior,
    voltage_vs_quality_avg,
    kwh_decile_quality_avg_quality_max_kwh,
    read_at_date,
    kwh_decile_quality_min_voltage_vs_quality_avg,
    read_at_dense_rank,
    ROUND(voltage * 1.07, 2) AS voltage_adj
FROM windowed_2
WHERE voltage_vs_quality_avg < 49.0
ORDER BY voltage_vs_quality_avg, id
LIMIT 5
