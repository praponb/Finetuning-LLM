WITH valid AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        CASE WHEN ms_played >= 282311 THEN 'A' WHEN ms_played >= 151112 THEN 'B' ELSE 'C' END AS ms_played_tier
    FROM plays
    WHERE ms_played > 71476
      AND track_id IS NOT NULL
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(ms_played) OVER (PARTITION BY device, listener_id ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ms_played_so_far,
        MAX(ms_played) OVER (PARTITION BY listener_id ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_ms_played_so_far,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY source, listener_id)) AS distinct_track_id_in_source_listener_id,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY source, listener_id), 2) AS ms_played_vs_source_listener_id_avg,
        ROUND(PERCENT_RANK() OVER w_all_by_ms_played_ties, 3) AS ms_played_percent_rank,
        DENSE_RANK() OVER (PARTITION BY listener_id ORDER BY ms_played, id) AS ms_played_dense_rank,
        SUM(ms_played) OVER (PARTITION BY listener_id ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played,
        COUNT(*) OVER (PARTITION BY device, listener_id ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        RANK() OVER (PARTITION BY listener_id, track_id ORDER BY ms_played DESC, id) AS ms_played_rank
    FROM valid
    WINDOW w_all_by_ms_played_ties AS (ORDER BY ms_played)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(ms_played_dense_rank) OVER w_all_by_min_ms_played_so_far AS prev_ms_played_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY distinct_track_id_in_source_listener_id, is_skipped ORDER BY distinct_track_id_in_source_listener_id DESC, id) AS distinct_track_id_in_source_listener_id_seq,
        FIRST_VALUE(ms_played) IGNORE NULLS OVER (ORDER BY min_ms_played_so_far, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played,
        SIZE(COLLECT_SET(listener_id) OVER w_ms_played_tier_track_id) AS distinct_listener_id_in_ms_played_tier_track_id,
        NTH_VALUE(ms_played_percent_rank, 2) OVER (ORDER BY min_ms_played_so_far, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played_percent_rank,
        ROUND(AVG(min_ms_played_so_far) OVER (PARTITION BY ms_played_tier, distinct_track_id_in_source_listener_id ORDER BY max_ms_played_so_far DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS min_ms_played_so_far_avg_7,
        ROW_NUMBER() OVER (ORDER BY ms_played_vs_source_listener_id_avg ASC, id) AS seq_no,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY distinct_track_id_in_source_listener_id, is_skipped ORDER BY distinct_track_id_in_source_listener_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played
    FROM windowed
    WINDOW w_all_by_min_ms_played_so_far AS (ORDER BY min_ms_played_so_far, id), w_ms_played_tier_track_id AS (PARTITION BY ms_played_tier, track_id)
)
-- Final projection
SELECT
    id,
    listener_id,
    device,
    is_skipped,
    source,
    ms_played_tier,
    min_ms_played_so_far,
    max_ms_played_so_far,
    distinct_track_id_in_source_listener_id,
    ms_played_dense_rank,
    running_ms_played,
    running_count,
    ms_played_rank,
    prev_ms_played_dense_rank,
    distinct_track_id_in_source_listener_id_seq,
    first_known_ms_played,
    distinct_listener_id_in_ms_played_tier_track_id,
    second_ms_played_percent_rank,
    seq_no,
    second_ms_played
FROM with_trends
ORDER BY running_count DESC, id
LIMIT 10
