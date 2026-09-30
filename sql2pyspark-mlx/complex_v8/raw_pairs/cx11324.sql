WITH base AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown,
        SUBSTRING(TRIM(source), 1, 4) AS source_prefix,
        ABS(ms_played - 168782) AS ms_played_distance
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-01-31 10:00:00'
      AND track_id IS NOT NULL
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        COUNT(*) FILTER (WHERE source = 'album') AS play_source_count
    FROM plays
    GROUP BY listener_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_source_count, 0) AS play_source_count
    FROM base s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.source,
        s.source_prefix,
        s.ms_played_distance,
        s.play_source_count,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.ms_played AS play_ms_played,
        p.device AS play_device
    FROM with_stats s
    JOIN plays p ON p.listener_id = s.listener_id
),
with_parent_play AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.source,
        s.source_prefix,
        s.play_source_count,
        s.play_id,
        s.play_played_at,
        s.play_ms_played,
        s.play_device,
        p.listener_id AS play_listener_id,
        p.is_skipped AS play_is_skipped,
        p.played_at AS play_played_at_2
    FROM detail_rows s
    JOIN plays p ON p.id = s.play_id
),
source_stats AS (
    -- Avg play_ms_played per source
    SELECT source, AVG(play_ms_played) AS avg_play_ms_played
    FROM with_parent_play
    GROUP BY source
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_play_ms_played,
        ROUND(s.play_ms_played - g.avg_play_ms_played, 2) AS play_ms_played_minus_avg
    FROM with_parent_play s
    JOIN source_stats g ON g.source = s.source
    WHERE s.play_ms_played > g.avg_play_ms_played
),
without_matches AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM above_average s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.device IS NOT NULL
              AND p.ms_played >= 56848
        )
)
SELECT *
FROM (
    SELECT play_played_at, device, avg_play_ms_played
    FROM without_matches
) src
PIVOT (
    SUM(avg_play_ms_played) AS amount, MAX(avg_play_ms_played) AS max_v
    FOR device IN ('car', 'desktop', 'phone')
)
ORDER BY play_played_at
