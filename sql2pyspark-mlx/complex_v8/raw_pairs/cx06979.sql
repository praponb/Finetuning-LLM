WITH valid AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        DATEDIFF(DATE '2025-12-31', played_at) AS days_since_played_at
    FROM plays
    WHERE device IS NOT NULL
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY device), 2) AS ms_played_vs_device_avg,
        ROUND(days_since_played_at - AVG(days_since_played_at) OVER (PARTITION BY listener_id), 2) AS days_since_played_at_vs_listener_id_avg,
        SUM(ms_played) OVER (PARTITION BY listener_id) AS listener_id_sum_ms_played,
        ROUND(AVG(ms_played) OVER w_listener_id_device, 2) AS listener_id_device_avg_ms_played,
        ROUND(days_since_played_at / NULLIF(SUM(days_since_played_at) OVER (PARTITION BY listener_id), 0), 4) AS days_since_played_at_share_of_listener_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY track_id ORDER BY ms_played ASC NULLS LAST), 3) AS ms_played_percent_rank,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY device, DATE_TRUNC('MONTH', played_at) ORDER BY days_since_played_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        DENSE_RANK() OVER (PARTITION BY listener_id ORDER BY played_at) AS played_at_dense_rank,
        MAX(days_since_played_at) OVER (PARTITION BY listener_id) AS listener_id_max_days_since_played_at,
        COUNT(*) OVER (PARTITION BY device, DATE_TRUNC('MONTH', played_at) ORDER BY days_since_played_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        FIRST_VALUE(played_at) OVER (PARTITION BY listener_id ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_played_at,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY listener_id ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played_2,
        ROUND(AVG(days_since_played_at) OVER (PARTITION BY device, DATE_TRUNC('MONTH', played_at) ORDER BY days_since_played_at NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS days_since_played_at_avg_1_prior,
        DENSE_RANK() OVER (PARTITION BY listener_id ORDER BY played_at DESC) AS played_at_dense_rank_2,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY listener_id), 0), 4) AS ms_played_share_of_listener_id
    FROM valid
    WINDOW w_listener_id_device AS (PARTITION BY listener_id, device)
),
sequenced_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        days_since_played_at,
        ms_played_vs_device_avg,
        listener_id_device_avg_ms_played,
        days_since_played_at_share_of_listener_id,
        second_ms_played,
        played_at_dense_rank,
        listener_id_max_days_since_played_at,
        first_played_at,
        second_ms_played_2,
        days_since_played_at_avg_1_prior,
        played_at_dense_rank_2,
        MIN(running_count) OVER (PARTITION BY listener_id) AS listener_id_min_running_count,
        ROW_NUMBER() OVER w_all_by_listener_id_max_days AS seq_no,
        played_at_dense_rank_2 - LAG(played_at_dense_rank_2) OVER (PARTITION BY played_at_dense_rank, listener_id ORDER BY days_since_played_at_avg_1_prior, id) AS played_at_dense_rank_2_change
    FROM sequenced
    WINDOW w_all_by_listener_id_max_days AS (ORDER BY listener_id_max_days_since_played_at ASC, id)
),
stats AS (
    -- Summarise per group
    SELECT
        played_at_dense_rank,
        listener_id,
        track_id,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids,
        ROUND(STDDEV(second_ms_played), 2) AS stddev_second_ms_played,
        COUNT(DISTINCT id) AS distinct_ids
    FROM sequenced_2
    WHERE played_at_dense_rank BETWEEN 4 AND 9
    GROUP BY played_at_dense_rank, listener_id, track_id
    HAVING AVG(second_ms_played) > 73625
),
sequenced_3 AS (
    -- Previous / next values per key
    SELECT
        played_at_dense_rank,
        listener_id,
        track_id,
        stddev_second_ms_played,
        distinct_ids,
        SUM(distinct_ids) OVER w_distinct_ids_distinct_listen AS distinct_ids_distinct_listener_ids_sum_distinct_,
        MAX(distinct_ids) OVER (PARTITION BY distinct_ids, distinct_listener_ids) AS distinct_ids_distinct_listener_ids_max_distinct_
    FROM stats
    WINDOW w_distinct_ids_distinct_listen AS (PARTITION BY distinct_ids, distinct_listener_ids)
)
-- Final projection
SELECT
    played_at_dense_rank,
    track_id,
    stddev_second_ms_played,
    distinct_ids,
    distinct_ids_distinct_listener_ids_sum_distinct_,
    distinct_ids - played_at_dense_rank AS distinct_ids_minus_played_at_dense_rank,
    ROUND(stddev_second_ms_played * 1.1, 2) AS stddev_second_ms_played_adj
FROM sequenced_3
ORDER BY stddev_second_ms_played
