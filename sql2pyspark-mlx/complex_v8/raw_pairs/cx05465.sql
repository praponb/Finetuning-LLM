WITH scoped_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        SUBSTRING(TRIM(source), 1, 4) AS source_prefix
    FROM plays
    WHERE (device = 'phone' OR is_skipped = FALSE)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        source_prefix,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY source, is_skipped), 0), 4) AS ms_played_share_of_source_is_skipped,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_id, track_id), 2) AS listener_id_track_id_avg_ms_played,
        DENSE_RANK() OVER (PARTITION BY track_id ORDER BY ms_played ASC, id) AS ms_played_dense_rank,
        RANK() OVER w_listener_id_source_by_ms_pla AS ms_played_rank,
        ROW_NUMBER() OVER w_track_id_source_by_ms_played AS rn,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_id, source ORDER BY ms_played DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_4,
        ms_played - LAG(ms_played) OVER (PARTITION BY listener_id, source ORDER BY ms_played DESC, id) AS ms_played_change,
        LEAD(ms_played) OVER (PARTITION BY is_skipped, device ORDER BY ms_played DESC, id) AS next_ms_played,
        ROUND(PERCENT_RANK() OVER w_all_by_ms_played_ties, 3) AS ms_played_percent_rank
    FROM scoped_plays
    WINDOW w_listener_id_source_by_ms_pla AS (PARTITION BY listener_id, source ORDER BY ms_played DESC, id), w_track_id_source_by_ms_played AS (PARTITION BY track_id, source ORDER BY ms_played, id), w_all_by_ms_played_ties AS (ORDER BY ms_played ASC)
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        source_prefix,
        ms_played_share_of_source_is_skipped,
        listener_id_track_id_avg_ms_played,
        ms_played_dense_rank,
        ms_played_rank,
        ms_played_avg_4,
        ms_played_change,
        next_ms_played,
        ms_played_percent_rank
    FROM sequenced
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    listener_id,
    device,
    is_skipped,
    ms_played_dense_rank,
    ms_played_rank,
    ms_played_avg_4,
    ms_played_change,
    next_ms_played,
    ms_played_percent_rank,
    IF(is_skipped, 'yes', 'no') AS is_skipped_label
FROM top_rows
ORDER BY ms_played_percent_rank
