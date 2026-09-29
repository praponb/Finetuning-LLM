WITH cleaned AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug_2
    FROM plays
    WHERE source = 'playlist'
),
stats AS (
    -- Summarise per group
    SELECT
        listener_id,
        source,
        ROUND(STDDEV(ms_played), 2) AS stddev_ms_played,
        MIN(played_at) AS first_played_at,
        COUNT(DISTINCT track_id) AS distinct_track_ids,
        SUM(ms_played) FILTER (WHERE source IN ('search', 'album')) AS ms_played_source_in_search_album,
        PERCENTILE_APPROX(ms_played, 0.5) AS median_ms_played
    FROM cleaned
    GROUP BY listener_id, source
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        COUNT(CASE WHEN source IS NOT NULL THEN 1 END) AS play_source_count,
        MAX(ms_played) AS max_play_ms_played,
        ROUND(AVG(ms_played), 2) AS avg_play_ms_played
    FROM plays
    GROUP BY listener_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_source_count, 0) AS play_source_count,
        p.max_play_ms_played,
        p.avg_play_ms_played
    FROM stats s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
play_stats_2 AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        COUNT(CASE WHEN is_skipped = TRUE THEN 1 END) AS play_is_skipped_count
    FROM plays
    GROUP BY listener_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_is_skipped_count, 0) AS play_is_skipped_count
    FROM summarized_rows s
    LEFT JOIN play_stats_2 p ON p.listener_id = s.listener_id
)
SELECT *
FROM (
    SELECT first_played_at, source, avg_play_ms_played
    FROM with_counts
) src
PIVOT (
    MIN(avg_play_ms_played)
    FOR source IN ('album', 'search', 'playlist')
)
ORDER BY first_played_at
