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
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        s.prior_rows_12h,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.track_id AS play_track_id,
        p.is_skipped AS play_is_skipped,
        p.played_at AS play_played_at
    FROM covered s
    JOIN plays p ON p.listener_id = s.listener_id AND p.played_at >= TIMESTAMP '2025-01-25 07:00:00'
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        play_id,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT play_track_id) AS distinct_play_track_ids
    FROM detail_rows
    GROUP BY 1
)
SELECT
    play_id,
    n_rows,
    distinct_play_track_ids,
    ROUND(n_rows / NULLIF(distinct_play_track_ids, 0), 3) AS n_rows_per_distinct_play_track_ids,
    ABS(distinct_play_track_ids - 6) AS distinct_play_track_ids_distance
FROM rollup_base
ORDER BY distinct_play_track_ids DESC, play_id
LIMIT 20
