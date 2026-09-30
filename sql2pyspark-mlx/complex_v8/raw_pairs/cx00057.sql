WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-02-12 13:00:00'
      AND kwh > 42.1
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        quality_len,
        site_id,
        MAX(quality_len) AS max_quality_len,
        ROUND(SUM(voltage) / NULLIF(SUM(quality_len), 0), 3) AS voltage_to_quality_len_ratio,
        ROUND(SUM(quality_len) / NULLIF(SUM(kwh), 0), 3) AS quality_len_to_kwh_ratio,
        ROUND(SUM(kwh) / NULLIF(SUM(quality_len), 0), 3) AS kwh_to_quality_len_ratio
    FROM recent_readings
    GROUP BY 1, 2
    HAVING MAX(voltage) > 218.4
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM totals s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
        )
)
-- Final projection
SELECT
    quality_len,
    site_id,
    max_quality_len,
    voltage_to_quality_len_ratio,
    quality_len_to_kwh_ratio,
    kwh_to_quality_len_ratio,
    COALESCE(max_quality_len, 0) AS max_quality_len_filled,
    kwh_to_quality_len_ratio + max_quality_len AS kwh_to_quality_len_ratio_plus_max_quality_len
FROM screened
