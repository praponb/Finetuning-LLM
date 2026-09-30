WITH base_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source,
        CONCAT_WS(': ', device, source) AS device_source_label,
        CASE WHEN ms_played >= 218129 THEN 'A' WHEN ms_played >= 73177 THEN 'B' ELSE 'C' END AS ms_played_tier,
        UPPER(TRIM(device)) AS device_clean
    FROM plays
    WHERE listener_id >= 10
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY YEAR(played_at) ORDER BY played_at, id) AS played_at_rank,
        LEAD(played_at, 2) OVER (PARTITION BY YEAR(played_at) ORDER BY played_at, id) AS next2_played_at,
        LAST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY YEAR(played_at) ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at,
        ROW_NUMBER() OVER w_all_by_played_at AS played_at_seq,
        FIRST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY YEAR(played_at) ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at
    FROM base_plays
    WINDOW w_all_by_played_at AS (ORDER BY played_at ASC, id)
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        COUNT(CASE WHEN is_skipped = FALSE THEN 1 END) AS play_is_skipped_count
    FROM plays
    GROUP BY track_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_is_skipped_count, 0) AS play_is_skipped_count
    FROM windowed s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
play_stats_2 AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        COUNT(*) FILTER (WHERE is_skipped = TRUE) AS play_is_skipped_count_2,
        COUNT(*) AS play_count,
        ROUND(AVG(ms_played), 2) AS avg_play_ms_played
    FROM plays
    GROUP BY track_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_is_skipped_count_2, 0) AS play_is_skipped_count_2,
        COALESCE(p.play_count, 0) AS play_count,
        p.avg_play_ms_played
    FROM with_stats s
    LEFT JOIN play_stats_2 p ON p.track_id = s.track_id
),
profiled_2 AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT SUM(p.ms_played) FROM plays p WHERE p.track_id = s.track_id) AS total_play_ms_played,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.track_id) AS play_count_2,
        (SELECT MIN(p.ms_played) FROM plays p WHERE p.track_id = s.track_id) AS min_play_ms_played
    FROM profiled s
),
play_stats_4 AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        MAX(ms_played) AS max_play_ms_played,
        MAX(ms_played) AS max_play_ms_played_2,
        MAX(played_at) AS last_played_at
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.max_play_ms_played,
        p.max_play_ms_played_2,
        p.last_played_at
    FROM profiled_2 s
    LEFT JOIN play_stats_4 p ON p.track_id = s.track_id
)
-- Subtotals with GROUPING SETS
SELECT
    listener_id,
    play_is_skipped_count,
    track_id,
    GROUPING(listener_id) AS is_total_listener_id,
    SUM(CASE WHEN is_skipped = TRUE THEN 1 ELSE 0 END) AS n_is_skipped_true
FROM summarized_rows
GROUP BY GROUPING SETS ((listener_id, play_is_skipped_count, track_id), (listener_id), (play_is_skipped_count), ())
