WITH joined_ranges AS (
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
        ROUND(AVG(prior_rows_7d), 2) AS avg_prior_rows_7d,
        MIN(read_at) AS first_read_at,
        MIN(prior_rows_7d) AS min_prior_rows_7d,
        ROUND(AVG(prior_voltage_7d), 2) AS avg_prior_voltage_7d,
        MAX(read_at) AS last_read_at,
        ROUND(SUM(prior_rows_7d) / NULLIF(SUM(prior_voltage_7d), 0), 3) AS prior_rows_7d_to_prior_voltage_7d_ratio
    FROM joined_ranges
    GROUP BY site_id, prior_rows_7d
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(avg_prior_rows_7d) OVER w_first_read_at_to_date, 2) AS first_read_at_to_date_avg_avg_prior_rows_7d,
        LAG(avg_prior_rows_7d) OVER w_min_prior_rows_7d_site_id_by AS prev_avg_prior_rows_7d
    FROM grouped
    WINDOW w_first_read_at_to_date AS (PARTITION BY TO_DATE(first_read_at)), w_min_prior_rows_7d_site_id_by AS (PARTITION BY min_prior_rows_7d, site_id ORDER BY first_read_at DESC, site_id, prior_rows_7d)
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM metrics s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
SELECT
    site_id,
    prior_rows_7d,
    first_read_at,
    min_prior_rows_7d,
    avg_prior_voltage_7d,
    last_read_at,
    prior_rows_7d_to_prior_voltage_7d_ratio,
    first_read_at_to_date_avg_avg_prior_rows_7d,
    prev_avg_prior_rows_7d,
    reading_count,
    DATE_TRUNC('YEAR', last_read_at) AS last_read_at_year
FROM with_stats
ORDER BY avg_prior_voltage_7d
