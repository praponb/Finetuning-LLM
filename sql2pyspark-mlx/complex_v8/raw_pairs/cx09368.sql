WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_12h,
        SUM(b.ms_played) AS prior_ms_played_12h
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 12 HOURS
    GROUP BY a.id, a.listener_id, a.played_at
),
joined_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.prior_rows_12h,
        s.prior_ms_played_12h,
        l.country AS listener_country,
        l.signup_date AS listener_signup_date,
        l.username AS listener_username
    FROM joined_ranges s
    JOIN listeners l ON l.id = s.listener_id
    WHERE l.signup_date BETWEEN DATE '2024-07-13' AND DATE '2025-03-18'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(prior_rows_12h) OVER w_listener_id_prior_rows_12h AS listener_id_prior_rows_12h_min_prior_rows_12h,
        NTILE(5) OVER (PARTITION BY listener_id, listener_country ORDER BY prior_ms_played_12h DESC NULLS FIRST, id) AS prior_ms_played_12h_quintile,
        COUNT(*) OVER w_listener_id_prior_rows_12h AS rows_in_listener_id_prior_rows_12h
    FROM joined_listener
    WINDOW w_listener_id_prior_rows_12h AS (PARTITION BY listener_id, prior_rows_12h)
)
-- Final projection
SELECT
    id,
    prior_rows_12h,
    prior_ms_played_12h,
    listener_country,
    listener_username,
    listener_id_prior_rows_12h_min_prior_rows_12h,
    prior_ms_played_12h_quintile,
    CASE WHEN listener_country IN ('UK', 'BR') THEN 'group_a' WHEN listener_country = 'TH' THEN 'group_b' ELSE 'other' END AS listener_country_group
FROM ranked
