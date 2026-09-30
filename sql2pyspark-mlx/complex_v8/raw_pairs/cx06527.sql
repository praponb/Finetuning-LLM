WITH joined_ranges AS (
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
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (PARTITION BY listener_id ORDER BY prior_rows_3d DESC, id))) / 60, 1) AS minutes_since_prev_played_at,
        ROUND(PERCENT_RANK() OVER w_listener_id_by_prior_rows_3d_2, 3) AS prior_rows_3d_percent_rank,
        LEAD(prior_ms_played_3d) OVER (PARTITION BY prior_rows_3d, listener_id ORDER BY prior_rows_3d, id) AS next_prior_ms_played_3d
    FROM joined_ranges
    WINDOW w_listener_id_by_prior_rows_3d_2 AS (PARTITION BY listener_id ORDER BY prior_rows_3d DESC)
),
enriched AS (
    -- Label and score rows
    SELECT *, HOUR(played_at) AS played_at_hour
    FROM sequenced
    WHERE minutes_since_prev_played_at > 1500.0
),
listener_id_stats AS (
    -- Max minutes_since_prev_played_at per listener_id
    SELECT listener_id, MAX(minutes_since_prev_played_at) AS max_minutes_since_prev_played_at
    FROM enriched
    GROUP BY listener_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_minutes_since_prev_played_at,
        ROUND(s.minutes_since_prev_played_at - g.max_minutes_since_prev_played_at, 2) AS minutes_since_prev_played_at_minus_max
    FROM enriched s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.minutes_since_prev_played_at = g.max_minutes_since_prev_played_at
)
SELECT
    id,
    played_at,
    prior_rows_3d,
    prior_ms_played_3d,
    minutes_since_prev_played_at,
    max_minutes_since_prev_played_at,
    minutes_since_prev_played_at_minus_max,
    played_at_hour + prior_ms_played_3d AS played_at_hour_plus_prior_ms_played_3d
FROM above_average
ORDER BY played_at
