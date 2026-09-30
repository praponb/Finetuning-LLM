WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        played_at + INTERVAL 1 DAYS AS played_at_plus_1d,
        MONTH(played_at) AS played_at_month
    FROM plays
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM base
    WHERE ms_played < (SELECT MAX(ms_played) FROM base)
),
line_level_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.played_at_plus_1d,
        p.id AS play_id,
        p.source AS play_source,
        p.played_at AS play_played_at,
        p.is_skipped AS play_is_skipped,
        p.device AS play_device
    FROM screened s
    JOIN plays p ON p.listener_id = s.listener_id AND p.device = 'car'
)
SELECT *
FROM (
    SELECT play_id, listener_id, play_source, ms_played
    FROM line_level_plays
) src
PIVOT (
    SUM(ms_played)
    FOR play_source IN ('album', 'radio')
)
ORDER BY play_id, listener_id
