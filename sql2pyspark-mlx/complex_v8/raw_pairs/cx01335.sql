WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        read_at + INTERVAL 7 DAYS AS read_at_plus_7d,
        kwh - voltage AS kwh_minus_voltage
    FROM meter_readings
    WHERE voltage < 233.6
      AND kwh BETWEEN 15.9 AND 113.0
),
quality_stats AS (
    -- Max kwh per quality
    SELECT quality, MAX(kwh) AS max_kwh
    FROM cleaned_readings
    GROUP BY quality
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_kwh, ROUND(s.kwh - g.max_kwh, 2) AS kwh_minus_max
    FROM cleaned_readings s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.kwh = g.max_kwh
),
detail_rows AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.read_at_plus_7d,
        s.kwh_minus_voltage,
        s.max_kwh,
        s.kwh_minus_max,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.quality AS reading_quality,
        m.read_at AS reading_read_at,
        m.kwh AS reading_kwh
    FROM outliers s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        read_at_plus_7d,
        kwh_minus_voltage,
        kwh_minus_max,
        reading_id,
        reading_voltage,
        reading_quality,
        reading_read_at,
        reading_kwh,
        LEAD(voltage) OVER (PARTITION BY reading_quality, TO_DATE(read_at) ORDER BY reading_kwh, id, reading_id) AS next_voltage,
        SUM(max_kwh) OVER w_quality AS quality_sum_max_kwh,
        ROUND(kwh_minus_max / NULLIF(SUM(kwh_minus_max) OVER (PARTITION BY quality), 0), 4) AS kwh_minus_max_share_of_quality,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality)) AS distinct_site_id_in_quality,
        NTH_VALUE(reading_kwh, 2) OVER (PARTITION BY reading_quality, TO_DATE(read_at) ORDER BY reading_kwh, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reading_kwh
    FROM detail_rows
    WINDOW w_quality AS (PARTITION BY quality)
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM with_windows s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
        )
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(reading_read_at)) / 3600, 1) AS hours_reading_read_at_to_read_at,
        ABS(next_voltage - 221.8) AS next_voltage_distance
    FROM qualified
),
with_windows_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        read_at,
        voltage,
        quality,
        read_at_plus_7d,
        kwh_minus_max,
        reading_id,
        reading_voltage,
        reading_quality,
        reading_read_at,
        reading_kwh,
        quality_sum_max_kwh,
        distinct_site_id_in_quality,
        hours_reading_read_at_to_read_at,
        next_voltage_distance,
        MAX(second_reading_kwh) OVER (PARTITION BY TO_DATE(read_at_plus_7d)) AS read_at_plus_7d_to_date_max_second_reading_kwh,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY TO_DATE(read_at_plus_7d))) AS distinct_quality_in_read_at_plus_7d_to_date
    FROM labeled
)
-- Final projection
SELECT
    id,
    read_at,
    voltage,
    read_at_plus_7d,
    kwh_minus_max,
    reading_id,
    reading_voltage,
    reading_quality,
    quality_sum_max_kwh,
    distinct_site_id_in_quality,
    hours_reading_read_at_to_read_at,
    read_at_plus_7d_to_date_max_second_reading_kwh,
    distinct_quality_in_read_at_plus_7d_to_date,
    FLOOR(voltage / 5) * 5 AS voltage_bucket,
    DATE_FORMAT(reading_read_at, 'MMM yyyy') AS reading_read_at_label
FROM with_windows_2
