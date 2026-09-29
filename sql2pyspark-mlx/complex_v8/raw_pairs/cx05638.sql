WITH joined_ranges AS (
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
latest AS (
    -- Keep the earliest row per listener_id
    SELECT
        id,
        listener_id,
        played_at,
        prior_rows_7d,
        prior_ms_played_7d
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY listener_id ORDER BY prior_ms_played_7d, id) AS recency_rank
        FROM joined_ranges
    ) ranked
    WHERE recency_rank = 1
),
joined AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.prior_rows_7d,
        s.prior_ms_played_7d,
        l.signup_date AS listener_signup_date,
        l.birth_year AS listener_birth_year
    FROM latest s
    JOIN listeners l ON l.id = s.listener_id
)
SELECT
    id,
    prior_rows_7d,
    prior_ms_played_7d,
    listener_signup_date,
    listener_birth_year,
    FLOOR(prior_ms_played_7d / 1000000) * 1000000 AS prior_ms_played_7d_bucket
FROM joined
ORDER BY id DESC
LIMIT 10
