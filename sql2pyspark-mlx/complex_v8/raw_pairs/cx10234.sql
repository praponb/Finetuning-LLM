WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM plays
    WHERE listener_id >= 16
      AND played_at < TIMESTAMP '2025-03-02 11:00:00'
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.source,
        p.id AS play_id,
        p.track_id AS play_track_id,
        p.is_skipped AS play_is_skipped
    FROM recent s
    LEFT JOIN plays p ON p.listener_id = s.listener_id
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        COUNT(CASE WHEN source IN ('album', 'radio', 'search') THEN 1 END) AS play_source_count
    FROM plays
    GROUP BY track_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_source_count, 0) AS play_source_count
    FROM with_children_plays s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
grouped AS (
    -- Summarise per group
    SELECT
        listener_id,
        ARRAY_SORT(COLLECT_SET(source)) AS source_list,
        COUNT(*) AS cnt
    FROM profiled
    WHERE track_id <= 12
    GROUP BY listener_id
    HAVING COUNT(*) >= 2
)
SELECT
    listener_id,
    source_list,
    cnt,
    SIZE(source_list) AS source_list_count
FROM grouped
