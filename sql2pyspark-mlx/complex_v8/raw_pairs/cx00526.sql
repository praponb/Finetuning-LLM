WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        SUBSTRING(TRIM(source), 1, 2) AS source_prefix,
        INITCAP(TRIM(source)) AS source_clean,
        DAYOFWEEK(played_at) AS played_at_dayofweek
    FROM plays
    WHERE device IS NOT NULL
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.source,
        s.source_prefix,
        s.played_at_dayofweek,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.played_at AS play_played_at,
        p.ms_played AS play_ms_played,
        p.source AS play_source
    FROM valid s
    JOIN plays p ON p.track_id = s.track_id AND TO_DATE(p.played_at) < DATE '2025-02-06'
)
SELECT id, play_id, metric, amount
FROM (
    SELECT id, play_id, play_ms_played, played_at_dayofweek
    FROM line_level
) src
UNPIVOT (
    amount FOR metric IN (play_ms_played AS play_ms_played_v, played_at_dayofweek AS played_at_dayofweek_v)
) unp
ORDER BY id, play_id, metric
