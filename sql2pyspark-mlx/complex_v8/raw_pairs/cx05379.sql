WITH base_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM plays
    WHERE (device IN ('speaker', 'phone', 'desktop') OR ms_played < 291723)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        played_at,
        is_skipped,
        source,
        ROW_NUMBER() OVER w_track_id_by_ms_played AS seq_no,
        ROUND(AVG(ms_played) OVER (PARTITION BY MONTH(played_at), device), 2) AS played_at_month_device_avg_ms_played,
        FIRST_VALUE(ms_played) OVER (ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played,
        SUM(ms_played) OVER (PARTITION BY track_id ORDER BY ms_played DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS ms_played_sum_6_prior,
        LEAD(played_at) OVER (PARTITION BY track_id ORDER BY ms_played DESC, id) AS next_played_at,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY YEAR(played_at)), 0), 4) AS ms_played_share_of_played_at_year,
        ROW_NUMBER() OVER w_is_skipped_source_by_played AS played_at_seq,
        DENSE_RANK() OVER (ORDER BY ms_played ASC NULLS LAST, id) AS ms_played_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY is_skipped, source ORDER BY played_at DESC), 3) AS played_at_cume_dist,
        DENSE_RANK() OVER (PARTITION BY track_id ORDER BY ms_played DESC, id) AS ms_played_dense_rank_2,
        ROW_NUMBER() OVER (PARTITION BY track_id, DATE_TRUNC('MONTH', played_at) ORDER BY ms_played, id) AS rn,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_played_at_month_device, 0), 4) AS ms_played_share_of_played_at_month_device,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY track_id, DATE_TRUNC('MONTH', played_at) ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played
    FROM base_plays
    WINDOW w_track_id_by_ms_played AS (PARTITION BY track_id ORDER BY ms_played DESC, id), w_is_skipped_source_by_played AS (PARTITION BY is_skipped, source ORDER BY played_at DESC, id), w_played_at_month_device AS (PARTITION BY MONTH(played_at), device)
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        played_at,
        is_skipped,
        source,
        seq_no,
        played_at_month_device_avg_ms_played,
        first_ms_played,
        ms_played_share_of_played_at_year,
        played_at_seq,
        ms_played_dense_rank,
        played_at_cume_dist,
        ms_played_dense_rank_2,
        second_ms_played,
        FLOOR(ms_played_dense_rank_2 / 2) * 2 AS ms_played_dense_rank_2_bucket,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(next_played_at)) / 3600, 1) AS hours_next_played_at_to_played_at,
        SUBSTRING(TRIM(source), 1, 3) AS source_prefix
    FROM windowed
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY ms_played_dense_rank) AS rows_in_ms_played_dense_rank,
        MAX(seq_no) OVER (PARTITION BY ms_played_dense_rank) AS ms_played_dense_rank_max_seq_no,
        FIRST_VALUE(ms_played_dense_rank_2) IGNORE NULLS OVER (PARTITION BY source ORDER BY ms_played_dense_rank_2_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played_dense_rank_2,
        LAG(second_ms_played) OVER (PARTITION BY source ORDER BY ms_played_dense_rank_2_bucket DESC, id) AS prev_second_ms_played
    FROM labeled
    WINDOW w_ms_played_dense_rank AS (PARTITION BY ms_played_dense_rank)
)
SELECT
    id,
    is_skipped,
    source,
    seq_no,
    played_at_month_device_avg_ms_played,
    ms_played_share_of_played_at_year,
    played_at_seq,
    ms_played_dense_rank,
    played_at_cume_dist,
    ms_played_dense_rank_2,
    second_ms_played,
    hours_next_played_at_to_played_at,
    source_prefix,
    rows_in_ms_played_dense_rank,
    ms_played_dense_rank_max_seq_no,
    first_known_ms_played_dense_rank_2,
    prev_second_ms_played,
    LOWER(REGEXP_REPLACE(TRIM(source_prefix), ' ', '_')) AS source_prefix_slug,
    IF(is_skipped, 'yes', 'no') AS is_skipped_label
FROM sequenced
WHERE ms_played_dense_rank_max_seq_no <= 60
ORDER BY id DESC
LIMIT 10
