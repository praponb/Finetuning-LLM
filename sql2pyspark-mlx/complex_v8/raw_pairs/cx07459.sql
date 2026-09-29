WITH cleaned AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        p.id AS play_id,
        p.listener_id AS play_listener_id,
        p.source AS play_source,
        p.device AS play_device
    FROM cleaned s
    JOIN plays p ON p.track_id = s.track_id AND p.source IN ('playlist', 'search', 'radio')
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, COUNT(*) AS play_count
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count
    FROM exploded_detail_plays s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        play_id,
        play_listener_id,
        play_source,
        play_count,
        ROUND(CUME_DIST() OVER (ORDER BY play_count ASC NULLS LAST), 3) AS play_count_cume_dist,
        MIN(play_count) OVER (PARTITION BY play_source, listener_id) AS play_source_listener_id_min_play_count
    FROM summarized_rows
)
SELECT
    id,
    listener_id,
    play_listener_id,
    play_source,
    play_count,
    play_count_cume_dist,
    play_source_listener_id_min_play_count,
    ROUND(play_count_cume_dist * 1.1, 2) AS play_count_cume_dist_adj,
    CASE WHEN play_count_cume_dist >= 0.8 THEN 'HIGH' WHEN play_count_cume_dist >= 0.2 THEN 'MEDIUM' ELSE 'LOW' END AS play_count_cume_dist_tier
FROM ranked
WHERE listener_id >= 10
ORDER BY play_count_cume_dist_adj DESC, play_count_cume_dist
