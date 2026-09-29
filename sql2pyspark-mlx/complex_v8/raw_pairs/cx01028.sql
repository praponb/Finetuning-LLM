WITH base AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix,
        YEAR(read_at) AS read_at_year,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
    WHERE quality <> 'OK'
      AND site_id >= 2
),
annotated AS (
    -- Add calculated columns
    SELECT *, UPPER(TRIM(quality)) AS quality_clean
    FROM base
),
enriched AS (
    -- Label and score rows
    SELECT *, DATE_TRUNC('DAY', read_at) AS read_at_day
    FROM annotated
    WHERE voltage <= 234.7
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    voltage,
    quality,
    quality_prefix,
    read_at_year,
    quality_len,
    quality_clean,
    read_at_day,
    COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
FROM enriched
ORDER BY site_id DESC NULLS FIRST
