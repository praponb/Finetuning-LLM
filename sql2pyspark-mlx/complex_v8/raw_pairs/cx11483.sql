WITH prepared_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        LOWER(TRIM(source)) AS source_clean
    FROM plays
    WHERE ms_played >= 52009
      AND track_id <= 4
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN p.is_skipped = FALSE THEN 1 END) FROM plays p WHERE p.listener_id = s.listener_id) AS play_is_skipped_count,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS first_played_at,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS first_played_at_2
    FROM prepared_plays s
),
qualified AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM with_stats s
    WHERE s.track_id IN (
            SELECT p.track_id
            FROM plays p
            WHERE p.listener_id IS NOT NULL
              AND p.is_skipped = FALSE
        )
)
-- Subtotals with CUBE
SELECT
    listener_id,
    track_id,
    GROUPING_ID() AS gid,
    ROUND(AVG(play_is_skipped_count), 2) AS avg_play_is_skipped_count,
    COUNT(DISTINCT listener_id) AS distinct_listener_ids,
    ARRAY_SORT(COLLECT_SET(source)) AS source_list
FROM qualified
GROUP BY CUBE(listener_id, track_id)
ORDER BY listener_id NULLS LAST, track_id NULLS LAST
