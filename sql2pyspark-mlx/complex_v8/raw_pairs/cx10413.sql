WITH base_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        SUBSTRING(TRIM(source), 1, 2) AS source_prefix,
        CONCAT(source, '#', CAST(id AS STRING)) AS source_ref
    FROM plays
    WHERE ms_played >= 97825
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT SUM(p.ms_played) FROM plays p WHERE p.track_id = s.track_id) AS total_play_ms_played,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.track_id) AS last_played_at
    FROM base_plays s
)
SELECT id, measure, value
FROM (
    SELECT id, ms_played, total_play_ms_played
    FROM with_stats
) src
UNPIVOT INCLUDE NULLS (
    value FOR measure IN (ms_played, total_play_ms_played)
) unp
