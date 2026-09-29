WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.kwh) AS prior_kwh_7d
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.site_id, a.read_at
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        TO_DATE(read_at) AS read_at_day,
        prior_rows_7d,
        site_id,
        MIN(read_at) AS first_read_at,
        ROUND(STDDEV(prior_kwh_7d), 2) AS stddev_prior_kwh_7d,
        PERCENTILE_APPROX(prior_kwh_7d, 0.5) AS median_prior_kwh_7d,
        COUNT(*) AS cnt
    FROM matched
    GROUP BY 1, 2, 3
    HAVING SUM(prior_kwh_7d) > 60.0
)
SELECT
    read_at_day,
    stddev_prior_kwh_7d,
    median_prior_kwh_7d,
    GREATEST(prior_rows_7d, cnt) AS greatest_prior_rows_7d_cnt
FROM per_group
ORDER BY median_prior_kwh_7d DESC NULLS FIRST
