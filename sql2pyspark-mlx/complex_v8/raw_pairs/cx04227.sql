WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        CONCAT_WS('|', device, source) AS device_source_label,
        CONCAT(source, '#', CAST(listener_id AS STRING)) AS source_ref,
        CONCAT(device, '#', CAST(ms_played AS STRING)) AS device_ref
    FROM plays
    WHERE listener_id IS NOT NULL
),
with_details_track AS (
    -- Enrich with dimension data
    SELECT s.*, t.title AS track_title, t.is_explicit AS track_is_explicit
    FROM prepared s
    JOIN tracks t ON t.id = s.track_id AND t.duration_sec BETWEEN 142 AND 357
),
with_children AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.source,
        s.source_ref,
        s.device_ref,
        s.track_is_explicit,
        p.id AS play_id,
        p.device AS play_device,
        p.ms_played AS play_ms_played,
        p.source AS play_source
    FROM with_details_track s
    JOIN plays p ON p.listener_id = s.listener_id
),
totals AS (
    -- Group-level totals
    SELECT
        listener_id,
        ARRAY_SORT(COLLECT_SET(play_source)) AS play_source_list,
        MIN(play_ms_played) AS min_play_ms_played,
        MAX(play_ms_played) AS max_play_ms_played
    FROM with_children
    WHERE play_device = 'speaker'
    GROUP BY listener_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(10) OVER (ORDER BY min_play_ms_played ASC, listener_id) AS min_play_ms_played_decile,
        max_play_ms_played - LAG(max_play_ms_played) OVER (ORDER BY min_play_ms_played ASC, listener_id) AS max_play_ms_played_change,
        ROUND(PERCENT_RANK() OVER w_listener_id_by_min_play_ms_p, 3) AS min_play_ms_played_percent_rank,
        LAG(min_play_ms_played, 2, 0) OVER (PARTITION BY listener_id ORDER BY max_play_ms_played, listener_id) AS prev2_min_play_ms_played,
        DENSE_RANK() OVER (ORDER BY min_play_ms_played ASC) AS min_play_ms_played_dense_rank,
        NTILE(4) OVER (PARTITION BY listener_id ORDER BY max_play_ms_played, listener_id) AS max_play_ms_played_quartile
    FROM totals
    WINDOW w_listener_id_by_min_play_ms_p AS (PARTITION BY listener_id ORDER BY min_play_ms_played)
),
ranked_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY max_play_ms_played_quartile, listener_id ORDER BY min_play_ms_played_decile DESC, listener_id) AS min_play_ms_played_decile_seq,
        min_play_ms_played - LAG(min_play_ms_played) OVER (PARTITION BY max_play_ms_played_quartile, listener_id ORDER BY min_play_ms_played_decile DESC, listener_id) AS min_play_ms_played_change,
        LEAD(min_play_ms_played, 3, 0) OVER w_max_play_ms_played_quartile AS next3_min_play_ms_played
    FROM ranked
    WINDOW w_max_play_ms_played_quartile AS (PARTITION BY max_play_ms_played_quartile, listener_id ORDER BY min_play_ms_played_decile DESC, listener_id)
)
SELECT
    listener_id,
    max_play_ms_played,
    min_play_ms_played_decile,
    max_play_ms_played_change,
    min_play_ms_played_percent_rank,
    prev2_min_play_ms_played,
    min_play_ms_played_dense_rank,
    max_play_ms_played_quartile,
    min_play_ms_played_decile_seq,
    min_play_ms_played_change,
    next3_min_play_ms_played,
    LEAST(prev2_min_play_ms_played, min_play_ms_played) AS least_prev2_min_play_ms_played_min_play_ms_playe,
    COALESCE(next3_min_play_ms_played, 0) AS next3_min_play_ms_played_filled
FROM ranked_2
WHERE max_play_ms_played < 261750
ORDER BY listener_id, min_play_ms_played_dense_rank DESC
LIMIT 20
