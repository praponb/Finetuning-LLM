WITH covered AS (
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
line_level_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.prior_rows_3d,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.source AS play_source
    FROM covered s
    LEFT JOIN plays p ON p.listener_id = s.listener_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        prior_rows_3d,
        play_id,
        play_played_at,
        ROUND((UNIX_TIMESTAMP(play_played_at) - UNIX_TIMESTAMP(LAG(play_played_at) OVER (ORDER BY play_played_at, id, play_id))) / 60, 1) AS minutes_since_prev_play_played_at,
        LAG(prior_rows_3d, 3) OVER (PARTITION BY YEAR(play_played_at), prior_rows_3d ORDER BY prior_rows_3d ASC, id, play_id) AS prev3_prior_rows_3d
    FROM line_level_plays
)
-- Final projection
SELECT
    id,
    listener_id,
    prior_rows_3d,
    play_id,
    play_played_at,
    minutes_since_prev_play_played_at,
    DATE_FORMAT(play_played_at, 'yyyy-MM-dd HH:00') AS play_played_at_label
FROM with_trends
ORDER BY play_id
