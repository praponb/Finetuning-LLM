WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE site_id <= 7
      AND read_at >= TIMESTAMP '2025-01-19 13:00:00'
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        kwh,
        quality_ref,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        LENGTH(TRIM(quality_ref)) AS quality_ref_len,
        CONCAT(quality_ref, '#', CAST(id AS STRING)) AS quality_ref_ref
    FROM scoped
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        quality_or_unknown,
        quality_ref_len,
        ROUND(AVG(quality_ref_len), 2) AS avg_quality_ref_len,
        ROUND(STDDEV(quality_ref_len), 2) AS stddev_quality_ref_len,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(AVG(kwh), 2) AS avg_kwh,
        COUNT(*) AS n_rows,
        PERCENTILE_APPROX(quality_ref_len, 0.5) AS median_quality_ref_len
    FROM labeled
    GROUP BY quality_or_unknown, quality_ref_len
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT n_rows, quality_or_unknown, median_quality_ref_len
    FROM grouped
) src
PIVOT (
    AVG(median_quality_ref_len)
    FOR quality_or_unknown IN ('EST', 'UNKNOWN', 'BAD')
)
