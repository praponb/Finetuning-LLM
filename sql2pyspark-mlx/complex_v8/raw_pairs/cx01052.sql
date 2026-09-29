WITH covered AS (
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
with_parent_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        s.prior_rows_12h,
        l.signup_date AS listener_signup_date
    FROM covered s
    JOIN listeners l ON l.id = s.listener_id
),
listener_id_stats AS (
    -- Avg prior_rows_12h per listener_id
    SELECT listener_id, AVG(prior_rows_12h) AS avg_prior_rows_12h
    FROM with_parent_listener
    GROUP BY listener_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_prior_rows_12h,
        ROUND(s.prior_rows_12h - g.avg_prior_rows_12h, 2) AS prior_rows_12h_minus_avg
    FROM with_parent_listener s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.prior_rows_12h > g.avg_prior_rows_12h
)
SELECT
    id,
    listener_id,
    played_at,
    listener_signup_date,
    avg_prior_rows_12h,
    prior_rows_12h_minus_avg,
    COALESCE(avg_prior_rows_12h, 0.0) AS avg_prior_rows_12h_filled
FROM outliers
ORDER BY avg_prior_rows_12h DESC, id
LIMIT 20
