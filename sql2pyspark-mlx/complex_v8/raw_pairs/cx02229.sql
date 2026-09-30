WITH in_window AS (
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
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER w_all_by_prior_rows_1d AS prior_rows_1d_tercile,
        ROUND(prior_rows_1d / NULLIF(SUM(prior_rows_1d) OVER (PARTITION BY prior_rows_1d, YEAR(read_at)), 0), 4) AS prior_rows_1d_share_of_prior_rows_1d_read_at_yea,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER w_all_by_prior_rows_1d)) / 60, 1) AS minutes_since_prev_read_at
    FROM in_window
    WINDOW w_all_by_prior_rows_1d AS (ORDER BY prior_rows_1d ASC, id)
),
site_id_stats AS (
    -- Median prior_voltage_1d per site_id
    SELECT site_id, PERCENTILE_APPROX(prior_voltage_1d, 0.5) AS median_prior_voltage_1d
    FROM with_windows
    GROUP BY site_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_prior_voltage_1d,
        ROUND(s.prior_voltage_1d - g.median_prior_voltage_1d, 2) AS prior_voltage_1d_minus_median
    FROM with_windows s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.prior_voltage_1d >= g.median_prior_voltage_1d
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY prior_rows_1d, prior_rows_1d_tercile) AS rows_in_prior_rows_1d_prior_rows_1d_tercile,
        ROUND(median_prior_voltage_1d / NULLIF(SUM(median_prior_voltage_1d) OVER (PARTITION BY prior_rows_1d, prior_rows_1d_tercile), 0), 4) AS median_prior_voltage_1d_share_of_prior_rows_1d_p,
        ROUND(AVG(median_prior_voltage_1d) OVER (PARTITION BY prior_rows_1d ORDER BY prior_rows_1d DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS median_prior_voltage_1d_avg_2
    FROM vs_group
)
SELECT
    id,
    site_id,
    read_at,
    prior_rows_1d,
    prior_voltage_1d,
    prior_rows_1d_tercile,
    prior_rows_1d_share_of_prior_rows_1d_read_at_yea,
    minutes_since_prev_read_at,
    median_prior_voltage_1d,
    rows_in_prior_rows_1d_prior_rows_1d_tercile,
    median_prior_voltage_1d_share_of_prior_rows_1d_p,
    median_prior_voltage_1d_avg_2,
    DATE_TRUNC('WEEK', read_at) AS read_at_week
FROM sequenced
ORDER BY prior_voltage_1d DESC, id
