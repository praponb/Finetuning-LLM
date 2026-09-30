WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-01-25 10:00:00'
      AND site_id IS NOT NULL
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh,
        DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label,
        LENGTH(TRIM(quality)) AS quality_len
    FROM prepared
),
quality_stats AS (
    -- Avg kwh per quality
    SELECT quality, AVG(kwh) AS avg_kwh
    FROM enriched
    GROUP BY quality
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_kwh, ROUND(s.kwh - g.avg_kwh, 2) AS kwh_minus_avg
    FROM enriched s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.kwh > g.avg_kwh
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM outliers
    WHERE kwh_minus_avg > (SELECT AVG(kwh_minus_avg) FROM outliers)
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        read_at,
        kwh,
        voltage,
        quality,
        voltage_per_kwh,
        quality_len,
        avg_kwh,
        kwh_minus_avg,
        YEAR(read_at) AS read_at_year
    FROM qualified
)
SELECT
    id,
    read_at,
    kwh,
    voltage,
    quality,
    quality_len,
    kwh_minus_avg,
    read_at_year,
    LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
    ABS(voltage - 227.3) AS voltage_distance
FROM scored
ORDER BY voltage, id
LIMIT 5
