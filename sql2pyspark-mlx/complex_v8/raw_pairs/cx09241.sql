WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.voltage) AS prior_voltage_1d
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.site_id, a.read_at
),
site_id_stats AS (
    -- Avg prior_rows_1d per site_id
    SELECT site_id, AVG(prior_rows_1d) AS avg_prior_rows_1d
    FROM joined_ranges
    GROUP BY site_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_prior_rows_1d,
        ROUND(s.prior_rows_1d - g.avg_prior_rows_1d, 2) AS prior_rows_1d_minus_avg
    FROM joined_ranges s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.prior_rows_1d > g.avg_prior_rows_1d
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    prior_rows_1d,
    prior_voltage_1d,
    TO_DATE(read_at) AS read_at_date,
    ABS(prior_voltage_1d - 367.5) AS prior_voltage_1d_distance
FROM above_average
