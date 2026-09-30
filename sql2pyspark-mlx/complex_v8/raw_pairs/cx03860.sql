WITH cleaned AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        CONCAT(source, '#', CAST(listener_id AS STRING)) AS source_ref,
        YEAR(played_at) AS played_at_year,
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM plays
    WHERE device = 'speaker'
      AND source IS NOT NULL
),
line_level_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.source_ref,
        s.ms_played_filled,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.source AS play_source,
        p.track_id AS play_track_id
    FROM cleaned s
    JOIN plays p ON p.listener_id = s.listener_id AND p.source IS NOT NULL
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(device, 'ALL') AS device,
    play_id,
    GROUPING_ID() AS gid,
    SUM(ms_played_filled) AS total_ms_played_filled
FROM line_level_plays
GROUP BY GROUPING SETS ((device, play_id), (device))
ORDER BY device NULLS LAST, play_id NULLS LAST
