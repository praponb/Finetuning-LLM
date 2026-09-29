WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
    WHERE site_id >= 9
      AND quality IN ('OK', 'EST')
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        quality,
        FLOOR(voltage / 5) * 5 AS voltage_bucket,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref,
        DATE_TRUNC('YEAR', read_at) AS read_at_year
    FROM filtered
),
enriched_2 AS (
    -- Derive helper columns
    SELECT
        id,
        read_at,
        quality,
        voltage_bucket,
        quality_ref,
        read_at_year,
        LENGTH(TRIM(quality)) AS quality_len,
        CONCAT(quality_ref, '#', CAST(voltage_bucket AS STRING)) AS quality_ref_ref
    FROM enriched
    WHERE quality <> 'EST'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        read_at,
        quality,
        voltage_bucket,
        quality_ref,
        read_at_year,
        quality_len,
        quality_ref_ref,
        ROUND(AVG(voltage_bucket) OVER (PARTITION BY quality, quality_len), 2) AS quality_quality_len_avg_voltage_bucket,
        NTILE(3) OVER (PARTITION BY MONTH(read_at), YEAR(read_at_year) ORDER BY quality_len, id) AS quality_len_tercile
    FROM enriched_2
)
-- Final projection
SELECT
    id,
    read_at,
    quality,
    voltage_bucket,
    quality_ref,
    quality_len,
    quality_ref_ref,
    quality_quality_len_avg_voltage_bucket,
    quality_len_tercile,
    ROUND(quality_len_tercile / NULLIF(voltage_bucket, 0), 3) AS quality_len_tercile_per_voltage_bucket,
    read_at + INTERVAL 7 DAYS AS read_at_plus_7d
FROM windowed
ORDER BY id DESC, quality_len_tercile DESC
