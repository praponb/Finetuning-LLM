WITH filtered_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        LEAST(kwh, voltage) AS least_kwh_voltage,
        INITCAP(TRIM(quality)) AS quality_clean,
        DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 8 AND 10
),
annotated AS (
    -- Derive helper columns
    SELECT *, read_at + INTERVAL 30 MINUTES AS read_at_plus_30m
    FROM filtered_readings
)
-- Subtotals with GROUPING SETS
SELECT
    site_id,
    quality,
    GROUPING(site_id) AS is_total_site_id,
    COUNT(DISTINCT id) AS distinct_ids
FROM annotated
GROUP BY GROUPING SETS ((site_id, quality), (site_id), (quality))
