WITH matched AS (
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
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM matched
    WHERE prior_rows_12h > (SELECT AVG(prior_rows_12h) FROM matched)
),
site_id_stats AS (
    -- Median prior_rows_12h per site_id
    SELECT site_id, PERCENTILE_APPROX(prior_rows_12h, 0.5) AS median_prior_rows_12h
    FROM screened
    GROUP BY site_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_prior_rows_12h,
        ROUND(s.prior_rows_12h - g.median_prior_rows_12h, 2) AS prior_rows_12h_minus_median
    FROM screened s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.prior_rows_12h >= g.median_prior_rows_12h
)
-- Final projection
SELECT
    id,
    read_at,
    prior_rows_12h,
    prior_voltage_12h,
    prior_rows_12h_minus_median,
    FLOOR(prior_voltage_12h / 250) * 250 AS prior_voltage_12h_bucket
FROM compared
ORDER BY read_at DESC NULLS FIRST
