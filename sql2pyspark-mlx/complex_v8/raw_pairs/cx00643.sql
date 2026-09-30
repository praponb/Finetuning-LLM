WITH filtered_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM plays
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY track_id, source_or_unknown ORDER BY played_at DESC, id) AS played_at_decile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY track_id, source_or_unknown ORDER BY played_at DESC), 3) AS played_at_percent_rank,
        LAG(ms_played, 3) OVER (PARTITION BY track_id, source_or_unknown ORDER BY played_at DESC, id) AS prev3_ms_played
    FROM filtered_plays
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        COUNT(*) AS play_count,
        MAX(ms_played) AS max_play_ms_played,
        COUNT(*) FILTER (WHERE device = 'desktop') AS play_device_count
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count,
        p.max_play_ms_played,
        COALESCE(p.play_device_count, 0) AS play_device_count
    FROM with_trends s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT SUM(p.ms_played) FROM plays p WHERE p.track_id = s.track_id) AS total_play_ms_played,
        (SELECT COUNT(*) FILTER (WHERE p.is_skipped = FALSE) FROM plays p WHERE p.track_id = s.track_id) AS play_is_skipped_count
    FROM summarized_rows s
)
SELECT
    play_is_skipped_count,
    track_id,
    ROUND(100 * COUNT(CASE WHEN source = 'search' THEN 1 END) / COUNT(*), 1) AS pct_source_search,
    SUM(CASE WHEN source NOT IN ('radio', 'search') THEN played_at_percent_rank ELSE 0 END) AS played_at_percent_rank_source_not_in_radio_searc
FROM profiled
GROUP BY ROLLUP(play_is_skipped_count, track_id)
