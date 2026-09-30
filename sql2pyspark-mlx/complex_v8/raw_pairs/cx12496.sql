WITH base_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE device <> 'phone'
      AND listener_id IS NOT NULL
),
track_id_stats AS (
    -- Avg ms_played per track_id
    SELECT track_id, AVG(ms_played) AS avg_ms_played
    FROM base_plays
    GROUP BY track_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ms_played,
        ROUND(s.ms_played - g.avg_ms_played, 2) AS ms_played_minus_avg
    FROM base_plays s
    JOIN track_id_stats g ON g.track_id = s.track_id
    WHERE s.ms_played > g.avg_ms_played
),
without_matches AS (
    -- Keep rows whose listener matches
    SELECT s.*
    FROM compared s
    WHERE EXISTS (
            SELECT 1
            FROM listeners l
            WHERE l.id = s.listener_id
              AND SIZE(l.interests) >= 2
              AND l.username IS NOT NULL
        )
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.is_skipped,
        s.source,
        s.ms_played_minus_avg,
        p.id AS play_id,
        p.device AS play_device,
        p.is_skipped AS play_is_skipped,
        p.ms_played AS play_ms_played
    FROM without_matches s
    LEFT JOIN plays p ON p.track_id = s.track_id
)
-- Final projection
SELECT
    id,
    played_at,
    is_skipped,
    source,
    ms_played_minus_avg,
    play_device,
    play_ms_played,
    DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at
FROM exploded_detail_plays
WHERE play_ms_played > 120663
ORDER BY id DESC
