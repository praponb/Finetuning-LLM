WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.ms_played) AS prior_ms_played_7d
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.listener_id, a.played_at
),
combined_details AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.prior_rows_7d,
        s.prior_ms_played_7d,
        l.birth_year AS listener_birth_year,
        l.username AS listener_username,
        l.signup_date AS listener_signup_date
    FROM covered s
    JOIN listeners l ON l.id = s.listener_id
    WHERE l.birth_year < 1988
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        listener_birth_year - prior_rows_7d AS listener_birth_year_minus_prior_rows_7d,
        prior_rows_7d + prior_ms_played_7d AS prior_rows_7d_plus_prior_ms_played_7d,
        GREATEST(listener_birth_year, prior_rows_7d) AS greatest_listener_birth_year_prior_rows_7d
    FROM combined_details
)
-- Final projection
SELECT
    id,
    prior_rows_7d,
    prior_ms_played_7d,
    listener_birth_year,
    listener_signup_date,
    listener_birth_year_minus_prior_rows_7d,
    prior_rows_7d_plus_prior_ms_played_7d
FROM tagged
WHERE prior_rows_7d_plus_prior_ms_played_7d > 200001
ORDER BY prior_rows_7d_plus_prior_ms_played_7d, id
LIMIT 10
