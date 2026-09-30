WITH cleaned AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        ABS(kwh - 83.8) AS kwh_distance,
        UPPER(TRIM(quality)) AS quality_clean,
        ROUND(kwh * 0.25, 2) AS kwh_adj
    FROM meter_readings
    WHERE TO_DATE(read_at) < DATE '2025-02-12'
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality_clean,
        LOWER(TRIM(quality_clean)) AS quality_clean_clean,
        FLOOR(kwh / 25) * 25 AS kwh_bucket
    FROM cleaned
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count
    FROM tagged s
)
-- Subtotals with GROUPING SETS
SELECT
    site_id,
    reading_count,
    MAX(voltage) AS max_voltage,
    ROUND(SUM(kwh), 2) AS total_kwh
FROM summarized_rows
GROUP BY GROUPING SETS ((site_id, reading_count), (site_id), ())
