WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'yyyy-MM-dd') AS read_at_label,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM meter_readings
    WHERE voltage < 232.7
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at,
        (SELECT MIN(m.kwh) FROM meter_readings m WHERE m.site_id = s.site_id) AS min_reading_kwh,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count
    FROM base_readings s
),
summary AS (
    -- Group-level totals
    SELECT
        quality_or_unknown,
        reading_count,
        DATE_FORMAT(last_reading_read_at, 'yyyy-MM') AS last_reading_read_at_ym,
        COUNT(DISTINCT quality) AS distinct_qualitys,
        COUNT(*) AS n_rows
    FROM profiled
    GROUP BY quality_or_unknown, reading_count, DATE_FORMAT(last_reading_read_at, 'yyyy-MM')
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(n_rows / 2) * 2 AS n_rows_bucket,
        LENGTH(TRIM(last_reading_read_at_ym)) AS last_reading_read_at_ym_len
    FROM summary
)
-- Final projection
SELECT
    quality_or_unknown,
    reading_count,
    last_reading_read_at_ym,
    distinct_qualitys,
    n_rows,
    n_rows_bucket,
    last_reading_read_at_ym_len
FROM enriched
ORDER BY reading_count NULLS LAST
