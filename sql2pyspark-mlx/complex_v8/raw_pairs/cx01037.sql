WITH scoped AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        LOWER(TRIM(source)) AS source_clean
    FROM plays
    WHERE device <> 'phone'
      AND TO_DATE(played_at) < DATE '2025-02-24'
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(p.ms_played), 2) FROM plays p WHERE p.track_id = s.track_id) AS avg_play_ms_played
    FROM scoped s
),
qualified AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM with_stats s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND NOT p.is_skipped
              AND p.listener_id >= 9
        )
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        avg_play_ms_played,
        ROUND(AVG(ms_played_bucket) OVER (PARTITION BY listener_id), 2) AS listener_id_avg_ms_played_bucket,
        MAX(ms_played) OVER (PARTITION BY listener_id) AS listener_id_max_ms_played
    FROM qualified
)
SELECT id, metric, amount
FROM (
    SELECT id, ms_played, listener_id_max_ms_played
    FROM metrics
) src
UNPIVOT (
    amount FOR metric IN (ms_played, listener_id_max_ms_played)
) unp
