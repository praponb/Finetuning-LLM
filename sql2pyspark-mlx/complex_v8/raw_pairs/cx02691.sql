WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
    WHERE kwh < 96.8
      AND site_id <= 2
),
quality_or_unknown_stats AS (
    -- Median voltage per quality_or_unknown
    SELECT quality_or_unknown, PERCENTILE_APPROX(voltage, 0.5) AS median_voltage
    FROM cleaned
    GROUP BY quality_or_unknown
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_voltage,
        ROUND(s.voltage - g.median_voltage, 2) AS voltage_minus_median
    FROM cleaned s
    JOIN quality_or_unknown_stats g ON g.quality_or_unknown = s.quality_or_unknown
    WHERE s.voltage >= g.median_voltage
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, COUNT(*) AS reading_count, MAX(read_at) AS last_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        m.last_reading_read_at
    FROM outliers s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
labeled AS (
    -- Derive helper columns
    SELECT *, LENGTH(TRIM(quality_or_unknown)) AS quality_or_unknown_len
    FROM summarized_rows
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT reading_count, read_at, quality_or_unknown, quality_or_unknown_len
    FROM labeled
) src
PIVOT (
    AVG(quality_or_unknown_len) AS avg_v, COUNT(quality_or_unknown_len) AS n
    FOR quality_or_unknown IN ('UNKNOWN', 'BAD', 'EST', 'OK')
)
