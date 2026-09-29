WITH base AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        ROUND(voltage * 1.5, 2) AS voltage_adj,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        FLOOR(kwh / 25) * 25 AS kwh_bucket
    FROM meter_readings
),
quality_stats AS (
    -- Max kwh_bucket per quality
    SELECT quality, MAX(kwh_bucket) AS max_kwh_bucket
    FROM base
    GROUP BY quality
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_kwh_bucket,
        ROUND(s.kwh_bucket - g.max_kwh_bucket, 2) AS kwh_bucket_minus_max
    FROM base s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.kwh_bucket = g.max_kwh_bucket
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(kwh_bucket_minus_max * 1.07, 2) AS kwh_bucket_minus_max_adj,
        CONCAT(quality, '#', CAST(kwh_bucket AS STRING)) AS quality_ref,
        LEAST(kwh_bucket_minus_max, voltage) AS least_kwh_bucket_minus_max_voltage
    FROM above_average
    WHERE quality_slug IS NOT NULL
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM derived s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.read_at < TIMESTAMP '2025-02-18 12:00:00'
              AND m.quality <> 'BAD'
        )
),
tagged AS (
    -- Derive helper columns
    SELECT *, ROUND(voltage_adj * 1.1, 2) AS voltage_adj_adj
    FROM screened
    WHERE max_kwh_bucket BETWEEN 12.0 AND 90.0
),
combined_details_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.voltage_adj,
        s.max_kwh_bucket,
        s.kwh_bucket_minus_max,
        s.kwh_bucket_minus_max_adj,
        s.quality_ref,
        s.least_kwh_bucket_minus_max_voltage,
        si.commissioned_on AS site_commissioned_on
    FROM tagged s
    JOIN sites si ON si.id = s.site_id
    WHERE si.commissioned_on >= DATE '2024-09-15'
)
SELECT
    id,
    site_id,
    quality,
    voltage_adj,
    max_kwh_bucket,
    quality_ref,
    COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
    ROUND(kwh_bucket_minus_max * 1.07, 2) AS kwh_bucket_minus_max_adj_2
FROM combined_details_site
ORDER BY quality_ref NULLS LAST
