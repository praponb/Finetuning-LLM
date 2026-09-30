WITH base AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        CASE WHEN ms_played >= 223262 THEN 'HIGH' WHEN ms_played >= 168782 THEN 'MEDIUM' ELSE 'LOW' END AS ms_played_tier,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label
    FROM plays
    WHERE device IN ('tv', 'phone')
),
joined_track AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.source,
        s.ms_played_tier,
        s.is_skipped_label,
        t.moods AS track_moods,
        l.interests AS listener_interests
    FROM base s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN listeners l ON l.id = s.listener_id
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        MIN(ms_played) AS min_play_ms_played,
        MAX(played_at) AS last_played_at,
        COUNT(*) AS play_count
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.min_play_ms_played,
        p.last_played_at,
        COALESCE(p.play_count, 0) AS play_count
    FROM joined_track s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
without_matches AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM summarized_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.is_skipped = FALSE
        )
)
SELECT
    listener_id,
    is_skipped_label,
    play_count,
    GROUPING_ID() AS gid,
    ROUND(AVG(ms_played), 2) AS avg_ms_played
FROM without_matches
GROUP BY CUBE(listener_id, is_skipped_label, play_count)
ORDER BY listener_id NULLS LAST, is_skipped_label NULLS LAST, play_count NULLS LAST
