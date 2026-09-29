WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.kwh) AS prior_kwh_3d
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.site_id, a.read_at
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', read_at) AS read_at_month,
        site_id,
        prior_rows_3d,
        ROUND(STDDEV(prior_rows_3d), 2) AS stddev_prior_rows_3d,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(STDDEV(prior_kwh_3d), 2) AS stddev_prior_kwh_3d,
        MIN(prior_kwh_3d) AS min_prior_kwh_3d,
        ROUND(AVG(prior_kwh_3d), 2) AS avg_prior_kwh_3d
    FROM in_window
    GROUP BY DATE_TRUNC('MONTH', read_at), site_id, prior_rows_3d
),
annotated AS (
    -- Derive helper columns
    SELECT
        read_at_month,
        site_id,
        prior_rows_3d,
        distinct_ids,
        stddev_prior_kwh_3d,
        min_prior_kwh_3d,
        read_at_month + INTERVAL 15 MINUTES AS read_at_month_plus_15m
    FROM summary
)
SELECT
    read_at_month,
    prior_rows_3d,
    distinct_ids,
    stddev_prior_kwh_3d,
    min_prior_kwh_3d,
    read_at_month_plus_15m,
    ABS(distinct_ids - 7) AS distinct_ids_distance
FROM annotated
ORDER BY prior_rows_3d DESC NULLS FIRST, stddev_prior_kwh_3d DESC
