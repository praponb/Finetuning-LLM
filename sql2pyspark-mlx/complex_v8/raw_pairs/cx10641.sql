WITH base_plays AS (
    -- Filter plays
    SELECT *
    FROM plays
    WHERE listener_id >= 3
      AND device = 'tv'
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.is_skipped,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.device AS play_device,
        p.source AS play_source,
        p.listener_id AS play_listener_id
    FROM base_plays s
    JOIN plays p ON p.track_id = s.track_id
)
-- Subtotals with GROUPING SETS
SELECT
    is_skipped,
    play_listener_id,
    GROUPING(is_skipped) AS is_total_is_skipped,
    ARRAY_SORT(COLLECT_SET(play_device)) AS play_device_list
FROM detail_rows
GROUP BY GROUPING SETS ((is_skipped, play_listener_id), (is_skipped), ())
ORDER BY is_skipped NULLS LAST, play_listener_id NULLS LAST
