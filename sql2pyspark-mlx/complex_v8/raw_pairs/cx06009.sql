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
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        read_at,
        prior_rows_12h,
        prior_kwh_12h,
        LEAST(prior_rows_12h, prior_kwh_12h) AS least_prior_rows_12h_prior_kwh_12h,
        DATE_TRUNC('DAY', read_at) AS read_at_day
    FROM joined_ranges
    WHERE prior_kwh_12h < 480.0
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        prior_rows_12h,
        prior_kwh_12h,
        read_at_day,
        LEAD(prior_rows_12h) OVER (ORDER BY prior_kwh_12h ASC, id) AS next_prior_rows_12h,
        ROW_NUMBER() OVER (ORDER BY prior_kwh_12h ASC, id) AS rn,
        SUM(prior_rows_12h) OVER (PARTITION BY YEAR(read_at_day)) AS read_at_day_year_sum_prior_rows_12h,
        FIRST_VALUE(prior_rows_12h) OVER (ORDER BY prior_kwh_12h ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_prior_rows_12h,
        MIN(prior_kwh_12h) OVER w_read_at_day_year AS read_at_day_year_min_prior_kwh_12h,
        NTILE(3) OVER (ORDER BY prior_kwh_12h ASC, id) AS prior_kwh_12h_tercile
    FROM annotated
    WINDOW w_read_at_day_year AS (PARTITION BY YEAR(read_at_day))
)
-- Subtotals with CUBE
SELECT
    site_id,
    prior_rows_12h,
    rn,
    COUNT(*) AS n_rows
FROM ranked
GROUP BY CUBE(site_id, prior_rows_12h, rn)
ORDER BY site_id NULLS LAST, prior_rows_12h NULLS LAST, rn NULLS LAST
