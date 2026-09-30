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
with_details_listener AS (
    -- Join listeners
    SELECT
        s.*,
        l.signup_date AS listener_signup_date,
        l.plan AS listener_plan,
        l.interests AS listener_interests
    FROM joined_ranges s
    JOIN listeners l ON l.id = s.listener_id
),
listener_id_stats AS (
    -- Max prior_rows_1d per listener_id
    SELECT listener_id, MAX(prior_rows_1d) AS max_prior_rows_1d
    FROM with_details_listener
    GROUP BY listener_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_prior_rows_1d,
        ROUND(s.prior_rows_1d - g.max_prior_rows_1d, 2) AS prior_rows_1d_minus_max
    FROM with_details_listener s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.prior_rows_1d = g.max_prior_rows_1d
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS last_played_at,
        (SELECT SUM(p.ms_played) FROM plays p WHERE p.listener_id = s.listener_id) AS total_play_ms_played
    FROM above_average s
)
SELECT
    id,
    listener_id,
    played_at,
    prior_rows_1d,
    prior_ms_played_1d,
    listener_signup_date,
    listener_plan,
    listener_interests,
    prior_rows_1d_minus_max,
    last_played_at,
    total_play_ms_played,
    LOWER(TRIM(listener_plan)) AS listener_plan_clean,
    GREATEST(total_play_ms_played, prior_rows_1d) AS greatest_total_play_ms_played_prior_rows_1d
FROM summarized_rows
WHERE prior_rows_1d >= 3
ORDER BY prior_rows_1d_minus_max DESC NULLS FIRST, played_at DESC NULLS FIRST
