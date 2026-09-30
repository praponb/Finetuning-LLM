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
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        s.prior_rows_12h,
        s.prior_ms_played_12h,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.is_skipped AS play_is_skipped
    FROM joined_ranges s
    JOIN plays p ON p.listener_id = s.listener_id
),
qualified AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM line_level s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.is_skipped = TRUE
        )
      AND s.played_at >= TIMESTAMP '2025-01-19 06:00:00'
)
SELECT
    id,
    listener_id,
    prior_rows_12h,
    prior_ms_played_12h,
    play_ms_played,
    play_is_skipped,
    prior_rows_12h + prior_ms_played_12h AS prior_rows_12h_plus_prior_ms_played_12h,
    played_at + INTERVAL 1 DAYS AS played_at_plus_1d
FROM qualified
ORDER BY prior_ms_played_12h, prior_rows_12h_plus_prior_ms_played_12h DESC
