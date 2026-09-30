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
site_id_stats AS (
    -- Avg prior_rows_7d per site_id
    SELECT site_id, AVG(prior_rows_7d) AS avg_prior_rows_7d
    FROM matched
    GROUP BY site_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_prior_rows_7d,
        ROUND(s.prior_rows_7d - g.avg_prior_rows_7d, 2) AS prior_rows_7d_minus_avg
    FROM matched s
    JOIN site_id_stats g ON g.site_id = s.site_id
    WHERE s.prior_rows_7d > g.avg_prior_rows_7d
),
site_id_stats_2 AS (
    -- Median prior_rows_7d_minus_avg per site_id
    SELECT site_id, PERCENTILE_APPROX(prior_rows_7d_minus_avg, 0.5) AS median_prior_rows_7d_minus_avg
    FROM vs_group
    GROUP BY site_id
),
vs_group_2 AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_prior_rows_7d_minus_avg,
        ROUND(s.prior_rows_7d_minus_avg - g.median_prior_rows_7d_minus_avg, 2) AS prior_rows_7d_minus_avg_minus_median
    FROM vs_group s
    JOIN site_id_stats_2 g ON g.site_id = s.site_id
    WHERE s.prior_rows_7d_minus_avg >= g.median_prior_rows_7d_minus_avg
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    prior_rows_7d,
    avg_prior_rows_7d,
    prior_rows_7d_minus_avg,
    median_prior_rows_7d_minus_avg,
    GREATEST(prior_rows_7d, prior_rows_7d_minus_avg) AS greatest_prior_rows_7d_prior_rows_7d_minus_avg
FROM vs_group_2
WHERE prior_rows_7d < 4
