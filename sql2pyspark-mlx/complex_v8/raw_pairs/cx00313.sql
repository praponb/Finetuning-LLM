WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.ms_played) AS prior_ms_played_1d
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.listener_id, a.played_at
),
with_parent AS (
    -- Join listeners
    SELECT s.*, l.username AS listener_username
    FROM joined_ranges s
    JOIN listeners l ON l.id = s.listener_id AND l.country IN ('KR', 'TH')
),
enriched_listener AS (
    -- Enrich with dimension data
    SELECT s.*, l.username AS listener_username_2
    FROM with_parent s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
with_parent_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.prior_rows_1d,
        s.prior_ms_played_1d,
        s.listener_username_2,
        l.signup_date AS listener_signup_date,
        l.interests AS listener_interests
    FROM enriched_listener s
    JOIN listeners l ON l.id = s.listener_id AND l.signup_date BETWEEN DATE '2024-04-16' AND DATE '2025-07-08'
    WHERE SIZE(l.interests) >= 2
)
-- Final projection
SELECT
    id,
    listener_username_2,
    listener_signup_date,
    ROUND(prior_ms_played_1d / NULLIF(prior_rows_1d, 0), 3) AS prior_ms_played_1d_per_prior_rows_1d
FROM with_parent_listener
ORDER BY listener_username_2 DESC, prior_ms_played_1d_per_prior_rows_1d NULLS LAST
