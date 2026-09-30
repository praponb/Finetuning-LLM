WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped
    FROM plays
    WHERE device = 'phone'
),
qualified AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM prepared s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.is_skipped = TRUE
        )
      AND NOT s.is_skipped
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        played_at,
        is_skipped,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_track_id_is_skipped, 0), 4) AS ms_played_share_of_track_id_is_skipped,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY is_skipped, listener_id ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        MIN(ms_played) OVER (PARTITION BY track_id, is_skipped) AS track_id_is_skipped_min_ms_played,
        NTILE(10) OVER (PARTITION BY track_id ORDER BY played_at, id) AS played_at_decile
    FROM qualified
    WINDOW w_track_id_is_skipped AS (PARTITION BY track_id, is_skipped)
)
SELECT id, metric_name, reading
FROM (
    SELECT id, played_at_decile, track_id_is_skipped_min_ms_played
    FROM sequenced
) src
UNPIVOT (
    reading FOR metric_name IN (played_at_decile, track_id_is_skipped_min_ms_played)
) unp
