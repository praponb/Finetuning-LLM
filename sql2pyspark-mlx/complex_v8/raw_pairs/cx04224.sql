WITH valid_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE NOT is_skipped
      AND HOUR(played_at) BETWEEN 10 AND 12
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.ms_played_bucket,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.track_id AS play_track_id,
        p.played_at AS play_played_at,
        p.is_skipped AS play_is_skipped
    FROM valid_plays s
    JOIN plays p ON p.listener_id = s.listener_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        track_id,
        ms_played,
        is_skipped,
        ms_played_bucket,
        play_id,
        play_track_id,
        play_played_at,
        SUM(ms_played_bucket) OVER (PARTITION BY play_id, track_id) AS play_id_track_id_sum_ms_played_bucket,
        ROUND(play_ms_played - AVG(play_ms_played) OVER (PARTITION BY play_id, track_id), 2) AS play_ms_played_vs_play_id_track_id_avg,
        NTILE(5) OVER w_listener_id_by_played_at AS played_at_quintile,
        MIN(play_ms_played) OVER (PARTITION BY play_id, track_id) AS play_id_track_id_min_play_ms_played,
        ROUND(play_ms_played / NULLIF(SUM(play_ms_played) OVER (PARTITION BY play_id, track_id), 0), 4) AS play_ms_played_share_of_play_id_track_id,
        COUNT(*) OVER w_play_id_track_id AS rows_in_play_id_track_id
    FROM detail_rows
    WINDOW w_listener_id_by_played_at AS (PARTITION BY listener_id ORDER BY played_at, id, play_id), w_play_id_track_id AS (PARTITION BY play_id, track_id)
),
screened AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.play_track_id
              AND p.listener_id <= 9
        )
)
-- Subtotals with CUBE
SELECT
    is_skipped,
    played_at_quintile,
    COUNT(*) AS n_rows
FROM screened
GROUP BY CUBE(is_skipped, played_at_quintile)
ORDER BY is_skipped NULLS LAST, played_at_quintile NULLS LAST
