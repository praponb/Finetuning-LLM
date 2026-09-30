WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
        ABS(kwh - 81.1) AS kwh_distance,
        DATE_TRUNC('DAY', read_at) AS read_at_day
    FROM meter_readings
    WHERE quality = 'BAD'
      AND kwh >= 52.4
),
detail_rows AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.quality_slug,
        s.kwh_distance,
        m.id AS reading_id,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage,
        m.quality AS reading_quality
    FROM recent_readings s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
),
summary AS (
    -- Summarise per group
    SELECT
        reading_id,
        reading_quality,
        COUNT(*) AS row_count,
        ROUND(SUM(kwh_distance), 2) AS total_kwh_distance
    FROM detail_rows
    WHERE reading_voltage BETWEEN 218.5 AND 233.6
    GROUP BY reading_id, reading_quality
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        reading_id,
        reading_quality,
        row_count,
        total_kwh_distance,
        RANK() OVER w_all_by_total_kwh_distance AS total_kwh_distance_rank,
        MAX(total_kwh_distance) OVER w_row_count_reading_quality AS row_count_reading_quality_max_total_kwh_distance,
        DENSE_RANK() OVER (ORDER BY row_count NULLS LAST, reading_id, reading_quality) AS row_count_dense_rank
    FROM summary
    WINDOW w_all_by_total_kwh_distance AS (ORDER BY total_kwh_distance DESC, reading_id, reading_quality), w_row_count_reading_quality AS (PARTITION BY row_count, reading_quality)
)
SELECT
    reading_id,
    reading_quality,
    row_count,
    total_kwh_distance,
    total_kwh_distance_rank,
    row_count_reading_quality_max_total_kwh_distance,
    row_count_dense_rank
FROM with_windows
ORDER BY row_count_reading_quality_max_total_kwh_distance
