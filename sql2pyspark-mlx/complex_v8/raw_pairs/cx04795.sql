WITH recent AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM recent
    WHERE voltage < (SELECT MAX(voltage) FROM recent)
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MIN(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS first_reading_read_at,
        (SELECT COUNT(CASE WHEN m.quality NOT IN ('EST', 'OK') THEN 1 END) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count
    FROM qualified s
),
stats AS (
    -- Summarise per group
    SELECT
        site_id,
        quality,
        DATE_TRUNC('MONTH', first_reading_read_at) AS first_reading_read_at_month,
        ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
        MAX(read_at) AS last_read_at,
        ROUND(SUM(voltage) / NULLIF(SUM(reading_quality_count), 0), 3) AS voltage_to_reading_quality_count_ratio,
        ROUND(STDDEV(reading_quality_count), 2) AS stddev_reading_quality_count
    FROM with_stats
    GROUP BY 1, 2, 3
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        ARRAY_JOIN(ARRAY_SORT(quality_list), ',') AS quality_list_text,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        YEAR(last_read_at) AS last_read_at_year
    FROM stats
)
SELECT
    site_id,
    quality,
    first_reading_read_at_month,
    voltage_to_reading_quality_count_ratio,
    quality_list_text,
    quality_or_unknown,
    last_read_at_year,
    COALESCE(quality_or_unknown, 'UNKNOWN') AS quality_or_unknown_or_unknown
FROM enriched
WHERE first_reading_read_at_month < TIMESTAMP '2025-02-17 09:00:00'
ORDER BY quality_list_text
