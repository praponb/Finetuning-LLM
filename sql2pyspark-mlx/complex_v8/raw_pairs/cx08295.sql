WITH cleaned AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE (site_id >= 5 OR kwh >= 29.2)
),
per_group AS (
    -- Group-level totals
    SELECT
        quality,
        site_id,
        COUNT(DISTINCT quality) AS distinct_qualitys,
        SUM(voltage) AS total_voltage,
        COUNT(*) AS row_count,
        SUM(CASE WHEN quality <> 'BAD' THEN kwh ELSE 0 END) AS kwh_quality_bad,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        ROUND(SUM(kwh) / NULLIF(SUM(voltage), 0), 3) AS kwh_to_voltage_ratio
    FROM cleaned
    GROUP BY quality, site_id
    HAVING COUNT(*) >= 3
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(*) FILTER (WHERE quality = 'EST') AS reading_quality_count,
        MAX(read_at) AS last_reading_read_at,
        MAX(read_at) AS last_reading_read_at_2
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count,
        m.last_reading_read_at,
        m.last_reading_read_at_2
    FROM per_group s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
derived AS (
    -- Derive helper columns
    SELECT
        quality,
        site_id,
        distinct_qualitys,
        total_voltage,
        row_count,
        kwh_quality_bad,
        quality_list,
        kwh_to_voltage_ratio,
        reading_quality_count,
        last_reading_read_at,
        last_reading_read_at_2,
        SUBSTRING(TRIM(quality), 1, 2) AS quality_prefix,
        DATE_FORMAT(last_reading_read_at, 'yyyy-MM') AS last_reading_read_at_label,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug
    FROM with_counts
)
SELECT
    quality,
    site_id,
    distinct_qualitys,
    row_count,
    kwh_quality_bad,
    quality_list,
    kwh_to_voltage_ratio,
    last_reading_read_at_2,
    quality_prefix,
    last_reading_read_at_label,
    quality_slug,
    DATEDIFF(DATE '2026-01-01', last_reading_read_at) AS days_since_last_reading_read_at
FROM derived
ORDER BY last_reading_read_at_2 DESC
