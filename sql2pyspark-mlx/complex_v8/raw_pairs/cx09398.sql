WITH valid AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_label,
        YEAR(played_at) AS played_at_year
    FROM plays
    WHERE (device IN ('desktop', 'speaker') OR NOT is_skipped)
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.source,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.device AS play_device,
        p.source AS play_source,
        p.listener_id AS play_listener_id
    FROM valid s
    JOIN plays p ON p.track_id = s.track_id
),
screened AS (
    -- Keep rows whose play matches
    SELECT s.*
    FROM line_level s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.id = s.play_id
              AND MONTH(p.played_at) IN (2, 3)
              AND p.device = 'car'
        )
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    device,
    source,
    play_id,
    play_ms_played,
    play_device,
    play_source,
    play_ms_played + ms_played AS play_ms_played_plus_ms_played,
    COALESCE(play_source, 'UNKNOWN') AS play_source_or_unknown
FROM screened
ORDER BY play_source_or_unknown NULLS LAST, ms_played
