WITH base_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE source = 'radio'
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, COUNT(*) AS play_count, MIN(ms_played) AS min_play_ms_played
    FROM plays
    GROUP BY track_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count,
        p.min_play_ms_played
    FROM base_plays s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.listener_id) AS play_count_2
    FROM with_counts s
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        device,
        source,
        play_count,
        play_count_2,
        MAX(ms_played) OVER (PARTITION BY TO_DATE(played_at), source) AS played_at_to_date_source_max_ms_played,
        ROW_NUMBER() OVER (ORDER BY played_at ASC, id) AS rn,
        ms_played - LAG(ms_played) OVER (PARTITION BY device ORDER BY ms_played ASC, id) AS ms_played_change,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY TO_DATE(played_at), source)) AS distinct_listener_id_in_played_at_to_date_source,
        ROUND(CUME_DIST() OVER (PARTITION BY play_count_2, play_count ORDER BY played_at ASC), 3) AS played_at_cume_dist,
        ROUND(min_play_ms_played / NULLIF(SUM(min_play_ms_played) OVER w_played_at_to_date_source, 0), 4) AS min_play_ms_played_share_of_played_at_to_date_so
    FROM with_stats
    WINDOW w_played_at_to_date_source AS (PARTITION BY TO_DATE(played_at), source)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        play_count_2,
        played_at_to_date_source_max_ms_played,
        distinct_listener_id_in_played_at_to_date_source,
        played_at_cume_dist,
        min_play_ms_played_share_of_played_at_to_date_so,
        RANK() OVER (PARTITION BY device, distinct_listener_id_in_played_at_to_date_source ORDER BY play_count DESC) AS play_count_rank,
        MAX(played_at_cume_dist) OVER (PARTITION BY play_count, device ORDER BY ms_played_change, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS played_at_cume_dist_max_3_prior,
        ROUND(PERCENT_RANK() OVER w_play_count_device_by_ms_play_2, 3) AS ms_played_change_percent_rank
    FROM with_trends
    WINDOW w_play_count_device_by_ms_play_2 AS (PARTITION BY play_count, device ORDER BY ms_played_change, id)
)
SELECT
    id,
    listener_id,
    track_id,
    play_count_2,
    distinct_listener_id_in_played_at_to_date_source,
    played_at_cume_dist,
    min_play_ms_played_share_of_played_at_to_date_so,
    play_count_rank,
    played_at_cume_dist_max_3_prior,
    ms_played_change_percent_rank,
    ROUND(played_at_cume_dist * 0.9, 2) AS played_at_cume_dist_adj,
    CASE WHEN play_count_rank >= 7 THEN 'large' WHEN play_count_rank >= 5 THEN 'medium' ELSE 'small' END AS play_count_rank_tier
FROM sequenced
ORDER BY played_at_cume_dist_max_3_prior NULLS LAST, play_count_rank_tier DESC, id
LIMIT 20
