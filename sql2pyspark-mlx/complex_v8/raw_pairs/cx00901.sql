WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'yyyy-MM-dd') AS read_at_label
    FROM meter_readings
    WHERE voltage <= 240.1
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT COUNT(CASE WHEN m.quality = 'BAD' THEN 1 END) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_quality_count,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count_2
    FROM base s
),
quality_stats AS (
    -- Max reading_count per quality
    SELECT quality, MAX(reading_count) AS max_reading_count
    FROM summarized_rows
    GROUP BY quality
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_reading_count,
        ROUND(s.reading_count - g.max_reading_count, 2) AS reading_count_minus_max
    FROM summarized_rows s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.reading_count = g.max_reading_count
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        voltage,
        read_at_label,
        reading_count,
        reading_count_2,
        reading_count_minus_max,
        ROUND(reading_count_2 - AVG(reading_count_2) OVER (PARTITION BY quality, site_id), 2) AS reading_count_2_vs_quality_site_id_avg,
        NTILE(10) OVER w_all_by_reading_count_minus_m AS reading_count_minus_max_decile
    FROM compared
    WINDOW w_all_by_reading_count_minus_m AS (ORDER BY reading_count_minus_max, id)
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        voltage,
        read_at_label,
        reading_count_2,
        reading_count_minus_max,
        reading_count_2_vs_quality_site_id_avg,
        COALESCE(reading_count_2, 0) AS reading_count_2_filled
    FROM ranked
),
annotated_2 AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        voltage,
        read_at_label,
        reading_count_2_vs_quality_site_id_avg,
        reading_count_2_filled,
        LEAST(reading_count_2_filled, voltage) AS least_reading_count_2_filled_voltage,
        COALESCE(voltage, 0.0) AS voltage_filled,
        GREATEST(voltage, reading_count_2_filled) AS greatest_voltage_reading_count_2_filled
    FROM annotated
)
SELECT
    id,
    site_id,
    voltage,
    voltage_filled,
    greatest_voltage_reading_count_2_filled,
    LOWER(REGEXP_REPLACE(TRIM(read_at_label), ' ', '_')) AS read_at_label_slug,
    LOWER(TRIM(read_at_label)) AS read_at_label_clean
FROM annotated_2
