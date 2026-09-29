WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source,
        CONCAT_WS('|', device, source) AS device_source_label,
        ABS(ms_played - 180879) AS ms_played_distance
    FROM plays
    WHERE source IS NOT NULL
      AND ms_played > 120663
),
screened AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM base s
    WHERE s.track_id IN (
            SELECT p.track_id
            FROM plays p
        )
      AND NOT s.is_skipped
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.is_skipped,
        s.source,
        s.ms_played_distance,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.source AS play_source
    FROM screened s
    JOIN plays p ON p.track_id = s.track_id AND p.ms_played > 42041
),
grouped AS (
    -- Group-level totals
    SELECT
        play_id,
        play_source,
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_ym,
        MIN(played_at) AS first_played_at,
        ROUND(STDDEV(ms_played_distance), 2) AS stddev_ms_played_distance,
        ROUND(100 * COUNT(CASE WHEN source = 'radio' THEN 1 END) / COUNT(*), 1) AS pct_source_radio,
        ARRAY_SORT(COLLECT_SET(source)) AS source_list,
        ROUND(100 * COUNT(CASE WHEN play_source NOT IN ('album', 'playlist') THEN 1 END) / COUNT(*), 1) AS pct_play_source_not_in_album_pla
    FROM detail_rows
    GROUP BY play_id, play_source, DATE_FORMAT(played_at, 'yyyy-MM')
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT play_id, play_source, stddev_ms_played_distance
    FROM grouped
) src
PIVOT (
    SUM(stddev_ms_played_distance)
    FOR play_source IN ('radio', 'album')
)
