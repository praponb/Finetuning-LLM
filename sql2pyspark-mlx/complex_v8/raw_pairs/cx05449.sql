WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.kwh) AS prior_kwh_12h
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.site_id, a.read_at
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        prior_rows_12h,
        site_id,
        COUNT(DISTINCT site_id) AS distinct_site_ids,
        PERCENTILE_APPROX(prior_kwh_12h, 0.5) AS median_prior_kwh_12h,
        SUM(prior_kwh_12h) AS total_prior_kwh_12h,
        MIN(prior_rows_12h) AS min_prior_rows_12h,
        PERCENTILE_APPROX(prior_rows_12h, 0.5) AS median_prior_rows_12h
    FROM joined_ranges
    GROUP BY prior_rows_12h, site_id
)
SELECT
    prior_rows_12h,
    site_id,
    distinct_site_ids,
    median_prior_kwh_12h,
    total_prior_kwh_12h,
    min_prior_rows_12h,
    median_prior_rows_12h,
    LEAST(prior_rows_12h, total_prior_kwh_12h) AS least_prior_rows_12h_total_prior_kwh_12h
FROM per_group
ORDER BY min_prior_rows_12h NULLS LAST, median_prior_kwh_12h
