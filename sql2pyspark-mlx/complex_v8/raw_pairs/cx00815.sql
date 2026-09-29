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
rollup_base AS (
    -- Group-level totals
    SELECT
        prior_rows_3d,
        listener_id,
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        PERCENTILE_APPROX(prior_rows_3d, 0.5) AS median_prior_rows_3d,
        ROUND(SUM(prior_rows_3d) / NULLIF(SUM(prior_ms_played_3d), 0), 3) AS prior_rows_3d_to_prior_ms_played_3d_ratio,
        MAX(played_at) AS last_played_at,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(prior_rows_3d) AS max_prior_rows_3d
    FROM in_window
    WHERE prior_ms_played_3d >= 200000
    GROUP BY prior_rows_3d, listener_id, DATE_TRUNC('MONTH', played_at)
    HAVING AVG(prior_rows_3d) > 1
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.prior_rows_3d,
        s.listener_id,
        s.played_at_month,
        s.median_prior_rows_3d,
        s.distinct_ids,
        l.signup_date AS listener_signup_date,
        l.plan AS listener_plan
    FROM rollup_base s
    JOIN listeners l ON l.id = s.listener_id
    WHERE l.plan IN ('family', 'student', 'premium')
)
-- Final projection
SELECT
    prior_rows_3d,
    listener_id,
    listener_signup_date,
    listener_plan
FROM with_details
ORDER BY listener_signup_date
