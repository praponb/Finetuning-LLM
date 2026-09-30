WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'yyyy-MM-dd HH:00') AS read_at_label,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix
    FROM meter_readings
    WHERE quality = 'EST'
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MAX(read_at) AS last_reading_read_at,
        COUNT(*) FILTER (WHERE quality <> 'BAD') AS reading_quality_count
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.last_reading_read_at,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count
    FROM cleaned_readings s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        kwh,
        voltage,
        read_at_label,
        quality_or_unknown,
        quality_prefix,
        last_reading_read_at,
        reading_quality_count,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(read_at) ORDER BY last_reading_read_at ASC) AS last_reading_read_at_dense_rank,
        ROUND(voltage / NULLIF(SUM(voltage) OVER (PARTITION BY reading_quality_count), 0), 4) AS voltage_share_of_reading_quality_count,
        FIRST_VALUE(last_reading_read_at) OVER (PARTITION BY TO_DATE(read_at) ORDER BY last_reading_read_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_last_reading_read_at
    FROM with_counts
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, amount
FROM (
    SELECT id, last_reading_read_at_dense_rank, reading_quality_count
    FROM with_windows
) src
UNPIVOT (
    amount FOR field IN (last_reading_read_at_dense_rank, reading_quality_count)
) unp
