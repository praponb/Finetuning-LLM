WITH filtered_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM plays
    WHERE played_at < TIMESTAMP '2025-02-18 12:00:00'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER w_source_by_ms_played AS rn,
        LAST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY is_skipped ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played,
        ROUND(PERCENT_RANK() OVER w_all_by_played_at_ties, 3) AS played_at_percent_rank,
        NTILE(10) OVER (PARTITION BY source ORDER BY ms_played, id) AS ms_played_decile,
        ROUND(CUME_DIST() OVER w_is_skipped_by_ms_played_ties, 3) AS ms_played_cume_dist,
        ROUND(CUME_DIST() OVER (PARTITION BY source ORDER BY ms_played), 3) AS ms_played_cume_dist_2,
        MAX(ms_played) OVER (PARTITION BY device ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_ms_played_so_far,
        DENSE_RANK() OVER (ORDER BY played_at ASC, id) AS played_at_dense_rank,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY track_id, DATE_TRUNC('MONTH', played_at)), 2) AS ms_played_vs_track_id_played_at_month_avg,
        LEAD(played_at) OVER w_all_by_played_at AS next_played_at,
        SIZE(COLLECT_SET(device) OVER (PARTITION BY track_id, DATE_TRUNC('MONTH', played_at))) AS distinct_device_in_track_id_played_at_month,
        COUNT(*) OVER (PARTITION BY is_skipped) AS rows_in_is_skipped,
        ROW_NUMBER() OVER (ORDER BY played_at ASC, id) AS played_at_seq,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY device, is_skipped), 2) AS ms_played_vs_device_is_skipped_avg,
        ROUND(ms_played - AVG(ms_played) OVER w_is_skipped, 2) AS ms_played_vs_is_skipped_avg
    FROM filtered_plays
    WINDOW w_source_by_ms_played AS (PARTITION BY source ORDER BY ms_played, id), w_all_by_played_at_ties AS (ORDER BY played_at ASC), w_is_skipped_by_ms_played_ties AS (PARTITION BY is_skipped ORDER BY ms_played DESC), w_all_by_played_at AS (ORDER BY played_at ASC, id), w_is_skipped AS (PARTITION BY is_skipped)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(rows_in_is_skipped - AVG(rows_in_is_skipped) OVER (PARTITION BY rn), 2) AS rows_in_is_skipped_vs_rn_avg,
        LAG(played_at) OVER w_all_by_next_played_at AS prev_played_at,
        LEAD(ms_played_cume_dist) OVER w_all_by_ms_played_decile AS next_ms_played_cume_dist,
        ROUND(played_at_dense_rank / NULLIF(SUM(played_at_dense_rank) OVER (PARTITION BY rn), 0), 4) AS played_at_dense_rank_share_of_rn,
        COUNT(*) OVER (PARTITION BY rn) AS rows_in_rn,
        LAST_VALUE(ms_played) OVER (ORDER BY next_played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played
    FROM with_trends
    WINDOW w_all_by_next_played_at AS (ORDER BY next_played_at, id), w_all_by_ms_played_decile AS (ORDER BY ms_played_decile, id), w_rn AS (PARTITION BY rn)
)
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    is_skipped,
    source,
    last_known_ms_played,
    ms_played_decile,
    ms_played_cume_dist,
    max_ms_played_so_far,
    played_at_dense_rank,
    ms_played_vs_track_id_played_at_month_avg,
    ms_played_vs_device_is_skipped_avg,
    ms_played_vs_is_skipped_avg,
    played_at_dense_rank_share_of_rn,
    rows_in_rn,
    last_ms_played,
    IF(is_skipped, 'yes', 'no') AS is_skipped_label,
    ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60, 1) AS minutes_prev_played_at_to_played_at
FROM with_windows
ORDER BY ms_played_decile
