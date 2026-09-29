WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.voltage) AS prior_voltage_7d
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.site_id, a.read_at
),
grouped AS (
    -- Summarise per group
    SELECT
        site_id,
        prior_rows_7d,
        COUNT(*) AS row_count,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(STDDEV(prior_rows_7d), 2) AS stddev_prior_rows_7d,
        MAX(read_at) AS last_read_at,
        SUM(prior_rows_7d) AS total_prior_rows_7d
    FROM covered
    GROUP BY site_id, prior_rows_7d
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(*) AS reading_count,
        ROUND(AVG(kwh), 2) AS avg_reading_kwh,
        COUNT(*) AS reading_count_2
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        m.avg_reading_kwh,
        COALESCE(m.reading_count_2, 0) AS reading_count_2
    FROM grouped s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
-- Final projection
SELECT
    site_id,
    prior_rows_7d,
    row_count,
    distinct_ids,
    stddev_prior_rows_7d,
    last_read_at,
    total_prior_rows_7d,
    reading_count,
    avg_reading_kwh,
    reading_count_2
FROM profiled
ORDER BY distinct_ids, reading_count_2
