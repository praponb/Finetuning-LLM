WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.voltage) AS prior_voltage_3d
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.site_id, a.read_at
),
tagged AS (
    -- Derive helper columns
    SELECT *, ROUND(prior_voltage_3d * 1.1, 2) AS prior_voltage_3d_adj
    FROM matched
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        site_id,
        read_at,
        prior_rows_3d,
        prior_voltage_3d,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY MONTH(read_at) ORDER BY prior_rows_3d, id))) / 60, 1) AS minutes_since_prev_read_at,
        MAX(prior_rows_3d) OVER w_read_at_month_site_id AS read_at_month_site_id_max_prior_rows_3d,
        COUNT(*) OVER w_read_at_month_site_id AS rows_in_read_at_month_site_id,
        ROUND(AVG(prior_rows_3d) OVER (PARTITION BY MONTH(read_at) ORDER BY prior_rows_3d, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS prior_rows_3d_avg_2
    FROM tagged
    WINDOW w_read_at_month_site_id AS (PARTITION BY DATE_TRUNC('MONTH', read_at), site_id)
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, COUNT(*) AS reading_count, MIN(read_at) AS first_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        m.first_reading_read_at
    FROM ranked s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    prior_rows_3d,
    minutes_since_prev_read_at,
    read_at_month_site_id_max_prior_rows_3d,
    rows_in_read_at_month_site_id,
    prior_rows_3d_avg_2,
    reading_count,
    first_reading_read_at,
    DATE_TRUNC('YEAR', first_reading_read_at) AS first_reading_read_at_year,
    DATEDIFF(DATE '2026-01-01', first_reading_read_at) AS days_since_first_reading_read_at
FROM with_counts
ORDER BY read_at_month_site_id_max_prior_rows_3d DESC NULLS FIRST, prior_rows_3d_avg_2 DESC NULLS FIRST
