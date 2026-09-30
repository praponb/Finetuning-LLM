WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.voltage) AS prior_voltage_12h
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.site_id, a.read_at
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        MAX(read_at) AS last_reading_read_at,
        MIN(read_at) AS first_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.last_reading_read_at,
        m.first_reading_read_at
    FROM covered s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        prior_rows_12h,
        prior_voltage_12h,
        last_reading_read_at,
        first_reading_read_at,
        MAX(prior_voltage_12h) OVER (PARTITION BY YEAR(last_reading_read_at), YEAR(first_reading_read_at) ORDER BY first_reading_read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_prior_voltage_12h_so_far,
        ROUND((UNIX_TIMESTAMP(first_reading_read_at) - UNIX_TIMESTAMP(LAG(first_reading_read_at) OVER (PARTITION BY YEAR(last_reading_read_at), YEAR(first_reading_read_at) ORDER BY first_reading_read_at DESC, id))) / 60, 1) AS minutes_since_prev_first_reading_read_at,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY YEAR(last_reading_read_at), YEAR(first_reading_read_at) ORDER BY first_reading_read_at DESC, id))) / 60, 1) AS minutes_since_prev_read_at,
        ROUND(AVG(prior_voltage_12h) OVER (PARTITION BY prior_rows_12h, site_id), 2) AS prior_rows_12h_site_id_avg_prior_voltage_12h
    FROM with_counts
),
site_id_stats AS (
    -- Max prior_voltage_12h per site_id
    SELECT site_id, MAX(prior_voltage_12h) AS max_prior_voltage_12h
    FROM ranked
    GROUP BY site_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_prior_voltage_12h,
        ROUND(s.prior_voltage_12h - g.max_prior_voltage_12h, 2) AS prior_voltage_12h_minus_max
    FROM ranked s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.prior_voltage_12h = g.max_prior_voltage_12h
)
-- Final projection
SELECT
    id,
    site_id,
    prior_rows_12h,
    prior_voltage_12h,
    last_reading_read_at,
    first_reading_read_at,
    max_prior_voltage_12h_so_far,
    minutes_since_prev_read_at,
    prior_rows_12h_site_id_avg_prior_voltage_12h,
    max_prior_voltage_12h,
    prior_voltage_12h_minus_max,
    DATEDIFF(DATE '2026-01-01', last_reading_read_at) AS days_since_last_reading_read_at
FROM outliers
ORDER BY prior_voltage_12h DESC
