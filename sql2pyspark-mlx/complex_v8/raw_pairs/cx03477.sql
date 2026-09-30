WITH scoped_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        LENGTH(TRIM(source)) AS source_len
    FROM plays
    WHERE listener_id IS NOT NULL
      AND device IN ('phone', 'speaker')
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        is_skipped,
        source,
        ROUND(AVG(source_len) OVER (PARTITION BY source, listener_id ORDER BY played_at ASC NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS source_len_avg_1_prior,
        NTH_VALUE(source_len, 2) OVER (PARTITION BY source, listener_id ORDER BY played_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_source_len,
        SUM(source_len) OVER (PARTITION BY source, listener_id ORDER BY played_at ASC NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS source_len_sum_2_prior,
        ROUND(AVG(source_len) OVER (PARTITION BY source, listener_id ORDER BY played_at ASC NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS source_len_avg_2,
        RANK() OVER (ORDER BY source_len NULLS LAST) AS source_len_rank,
        ROUND(AVG(source_len) OVER (PARTITION BY source, listener_id ORDER BY played_at ASC NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS source_len_avg_4,
        source_len - LAG(source_len) OVER (PARTITION BY source, listener_id ORDER BY played_at ASC NULLS LAST, id) AS source_len_change,
        LAST_VALUE(played_at) OVER (PARTITION BY track_id, source ORDER BY source_len DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at,
        ROUND(CUME_DIST() OVER (PARTITION BY track_id, source ORDER BY source_len DESC NULLS FIRST, id), 3) AS source_len_cume_dist,
        DENSE_RANK() OVER (ORDER BY source_len DESC, id) AS source_len_dense_rank,
        ROW_NUMBER() OVER w_listener_id_by_played_at AS rn,
        LAST_VALUE(played_at) OVER (ORDER BY source_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at_2,
        MIN(source_len) OVER (ORDER BY source_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_source_len_so_far,
        SUM(source_len) OVER (PARTITION BY listener_id ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_source_len,
        NTILE(3) OVER (ORDER BY source_len DESC, id) AS source_len_tercile
    FROM scoped_plays
    WINDOW w_listener_id_by_played_at AS (PARTITION BY listener_id ORDER BY played_at ASC, id)
),
ranked_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(source_len_tercile) IGNORE NULLS OVER (PARTITION BY source_len_dense_rank, source_len_rank ORDER BY second_source_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_source_len_tercile,
        SIZE(COLLECT_SET(source) OVER w_rn_second_source_len) AS distinct_source_in_rn_second_source_len,
        ROUND(PERCENT_RANK() OVER w_source_len_dense_rank_source_2, 3) AS second_source_len_percent_rank,
        ROUND(second_source_len - AVG(second_source_len) OVER (PARTITION BY source_len_rank), 2) AS second_source_len_vs_source_len_rank_avg,
        ROUND(min_source_len_so_far / NULLIF(SUM(min_source_len_so_far) OVER (PARTITION BY rn, second_source_len), 0), 4) AS min_source_len_so_far_share_of_rn_second_source_,
        NTH_VALUE(min_source_len_so_far, 2) OVER (PARTITION BY source_len_dense_rank, source_len_rank ORDER BY second_source_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_min_source_len_so_far,
        NTILE(3) OVER (PARTITION BY rn ORDER BY source_len_avg_2, id) AS source_len_avg_2_tercile,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY rn, second_source_len)) AS distinct_listener_id_in_rn_second_source_len
    FROM ranked
    WINDOW w_rn_second_source_len AS (PARTITION BY rn, second_source_len), w_source_len_dense_rank_source_2 AS (PARTITION BY source_len_dense_rank, source_len_rank ORDER BY second_source_len DESC, id)
)
SELECT
    id,
    listener_id,
    track_id,
    source_len_avg_1_prior,
    second_source_len,
    source_len_sum_2_prior,
    source_len_avg_2,
    source_len_avg_4,
    last_played_at,
    source_len_cume_dist,
    source_len_dense_rank,
    rn,
    last_played_at_2,
    min_source_len_so_far,
    running_source_len,
    source_len_tercile,
    second_source_len_percent_rank,
    second_source_len_vs_source_len_rank_avg,
    min_source_len_so_far_share_of_rn_second_source_,
    source_len_avg_2_tercile,
    distinct_listener_id_in_rn_second_source_len,
    DAYOFWEEK(last_played_at) AS last_played_at_dayofweek,
    CASE WHEN source_len_avg_2 >= 11.0 THEN 'HIGH' WHEN source_len_avg_2 >= 5.0 THEN 'MEDIUM' ELSE 'LOW' END AS source_len_avg_2_tier
FROM ranked_2
