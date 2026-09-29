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
grouped AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        MIN(prior_voltage_12h) AS min_prior_voltage_12h,
        COUNT(*) AS n_rows,
        MIN(read_at) AS first_read_at,
        SUM(prior_rows_12h) AS total_prior_rows_12h,
        ROUND(STDDEV(prior_voltage_12h), 2) AS stddev_prior_voltage_12h
    FROM covered
    GROUP BY 1
),
metrics AS (
    -- Previous / next values per key
    SELECT
        read_at_month,
        min_prior_voltage_12h,
        n_rows,
        first_read_at,
        total_prior_rows_12h,
        stddev_prior_voltage_12h,
        NTH_VALUE(read_at_month, 2) OVER (PARTITION BY n_rows ORDER BY n_rows DESC, read_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_read_at_month,
        NTILE(10) OVER w_n_rows_by_n_rows AS n_rows_decile,
        COUNT(*) OVER w_first_read_at_month_n_rows AS rows_in_first_read_at_month_n_rows,
        ROUND(AVG(n_rows) OVER (PARTITION BY DATE_TRUNC('MONTH', first_read_at), n_rows), 2) AS first_read_at_month_n_rows_avg_n_rows,
        ROUND((UNIX_TIMESTAMP(read_at_month) - UNIX_TIMESTAMP(LAG(read_at_month) OVER (PARTITION BY n_rows ORDER BY n_rows DESC NULLS FIRST, read_at_month))) / 60, 1) AS minutes_since_prev_read_at_month,
        MAX(stddev_prior_voltage_12h) OVER (PARTITION BY n_rows ORDER BY n_rows DESC NULLS FIRST, read_at_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS stddev_prior_voltage_12h_max_3_prior
    FROM grouped
    WINDOW w_n_rows_by_n_rows AS (PARTITION BY n_rows ORDER BY n_rows DESC, read_at_month), w_first_read_at_month_n_rows AS (PARTITION BY DATE_TRUNC('MONTH', first_read_at), n_rows)
)
-- Final projection
SELECT
    read_at_month,
    min_prior_voltage_12h,
    n_rows,
    first_read_at,
    stddev_prior_voltage_12h,
    second_read_at_month,
    n_rows_decile,
    rows_in_first_read_at_month_n_rows,
    first_read_at_month_n_rows_avg_n_rows,
    stddev_prior_voltage_12h_max_3_prior
FROM metrics
ORDER BY second_read_at_month, first_read_at_month_n_rows_avg_n_rows, read_at_month
LIMIT 20
