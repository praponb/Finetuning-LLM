WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.ms_played) AS prior_ms_played_3d
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.listener_id, a.played_at
),
listener_id_stats AS (
    -- Median prior_ms_played_3d per listener_id
    SELECT listener_id, PERCENTILE_APPROX(prior_ms_played_3d, 0.5) AS median_prior_ms_played_3d
    FROM in_window
    GROUP BY listener_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_prior_ms_played_3d,
        ROUND(s.prior_ms_played_3d - g.median_prior_ms_played_3d, 2) AS prior_ms_played_3d_minus_median
    FROM in_window s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.prior_ms_played_3d >= g.median_prior_ms_played_3d
)
SELECT
    id,
    listener_id,
    played_at,
    prior_ms_played_3d,
    median_prior_ms_played_3d,
    prior_ms_played_3d_minus_median,
    CASE WHEN prior_rows_3d >= 6 THEN 'large' WHEN prior_rows_3d >= 2 THEN 'medium' ELSE 'small' END AS prior_rows_3d_tier,
    FLOOR(prior_ms_played_3d / 1000000) * 1000000 AS prior_ms_played_3d_bucket
FROM above_average
ORDER BY played_at, prior_rows_3d_tier NULLS LAST
