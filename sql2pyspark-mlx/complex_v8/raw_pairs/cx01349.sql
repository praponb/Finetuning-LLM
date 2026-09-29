WITH in_window AS (
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
per_group AS (
    -- Summarise per group
    SELECT
        prior_rows_12h,
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        site_id,
        PERCENTILE_APPROX(prior_voltage_12h, 0.5) AS median_prior_voltage_12h,
        ROUND(AVG(prior_rows_12h), 2) AS avg_prior_rows_12h
    FROM in_window
    WHERE prior_voltage_12h > 306.2
    GROUP BY prior_rows_12h, DATE_TRUNC('MONTH', read_at), site_id
)
-- Final projection
SELECT prior_rows_12h, read_at_month, site_id
FROM per_group
ORDER BY read_at_month DESC, prior_rows_12h DESC
