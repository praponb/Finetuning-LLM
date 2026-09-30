WITH base AS (
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
    WHERE listener_id IS NOT NULL
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        played_at,
        device,
        is_skipped,
        source,
        ROW_NUMBER() OVER (PARTITION BY is_skipped, source ORDER BY ms_played DESC, id) AS ms_played_seq,
        DENSE_RANK() OVER w_is_skipped_by_ms_played AS ms_played_dense_rank,
        ROUND(AVG(ms_played) OVER (ORDER BY played_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_3,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY device), 0), 4) AS ms_played_share_of_device,
        SUM(ms_played) OVER (PARTITION BY listener_id) AS listener_id_sum_ms_played,
        NTILE(3) OVER (ORDER BY played_at, id) AS played_at_tercile,
        COUNT(*) OVER (ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MIN(ms_played) OVER w_device AS device_min_ms_played,
        MIN(ms_played) OVER (ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ms_played_so_far,
        RANK() OVER (PARTITION BY listener_id, source ORDER BY ms_played, id) AS ms_played_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY is_skipped ORDER BY ms_played, id), 3) AS ms_played_cume_dist,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY track_id), 2) AS ms_played_vs_track_id_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_id, source ORDER BY ms_played, id), 3) AS ms_played_cume_dist_2,
        ROUND(AVG(ms_played) OVER (PARTITION BY is_skipped ORDER BY ms_played, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_3_prior,
        FIRST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY is_skipped ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at
    FROM base
    WINDOW w_is_skipped_by_ms_played AS (PARTITION BY is_skipped ORDER BY ms_played, id), w_device AS (PARTITION BY device)
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        played_at,
        device,
        is_skipped,
        source,
        ms_played_seq,
        ms_played_dense_rank,
        ms_played_share_of_device,
        listener_id_sum_ms_played,
        running_count,
        device_min_ms_played,
        min_ms_played_so_far,
        ms_played_rank,
        ms_played_cume_dist,
        ms_played_vs_track_id_avg,
        ms_played_cume_dist_2,
        ms_played_avg_3_prior,
        first_known_played_at,
        TO_DATE(played_at) AS played_at_date,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag
    FROM sequenced
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_all_by_ms_played_vs_track_id, 3) AS ms_played_vs_track_id_avg_percent_rank,
        RANK() OVER (ORDER BY ms_played_vs_track_id_avg ASC, id) AS ms_played_vs_track_id_avg_rank,
        COUNT(*) OVER (PARTITION BY ms_played_rank ORDER BY ms_played_share_of_device, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_2,
        ROUND(AVG(ms_played_rank) OVER (PARTITION BY ms_played_rank ORDER BY ms_played_share_of_device, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS ms_played_rank_avg_2_prior,
        LAG(ms_played_rank) OVER w_is_skipped_ms_played_seq_by AS prev_ms_played_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY is_skipped, ms_played_seq ORDER BY min_ms_played_so_far DESC, id), 3) AS min_ms_played_so_far_cume_dist
    FROM scored
    WINDOW w_all_by_ms_played_vs_track_id AS (ORDER BY ms_played_vs_track_id_avg ASC), w_is_skipped_ms_played_seq_by AS (PARTITION BY is_skipped, ms_played_seq ORDER BY min_ms_played_so_far DESC, id)
)
-- Final projection
SELECT
    id,
    played_at,
    device,
    is_skipped,
    source,
    ms_played_seq,
    ms_played_share_of_device,
    listener_id_sum_ms_played,
    running_count,
    device_min_ms_played,
    ms_played_rank,
    ms_played_cume_dist,
    ms_played_vs_track_id_avg,
    ms_played_cume_dist_2,
    ms_played_avg_3_prior,
    played_at_date,
    ms_played_vs_track_id_avg_percent_rank,
    ms_played_vs_track_id_avg_rank,
    running_count_2,
    prev_ms_played_rank,
    min_ms_played_so_far_cume_dist,
    ms_played_cume_dist - running_count_2 AS ms_played_cume_dist_minus_running_count_2
FROM metrics
ORDER BY id DESC, ms_played_vs_track_id_avg_rank DESC
