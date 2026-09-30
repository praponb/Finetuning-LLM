WITH base_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        UPPER(TRIM(device)) AS device_clean
    FROM plays
    WHERE played_at < TIMESTAMP '2025-02-18 10:00:00'
      AND NOT is_skipped
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        listener_id,
        device,
        is_skipped,
        source,
        is_skipped_flag,
        ms_played_bucket,
        device_clean,
        LENGTH(TRIM(source)) AS source_len
    FROM base_plays
),
source_stats AS (
    -- Max is_skipped_flag per source
    SELECT source, MAX(is_skipped_flag) AS max_is_skipped_flag
    FROM tagged
    GROUP BY source
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_is_skipped_flag,
        ROUND(s.is_skipped_flag - g.max_is_skipped_flag, 2) AS is_skipped_flag_minus_max
    FROM tagged s
    JOIN source_stats g ON g.source = s.source
    WHERE s.is_skipped_flag = g.max_is_skipped_flag
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY is_skipped_flag, source_len ORDER BY is_skipped_flag_minus_max NULLS LAST, id), 3) AS is_skipped_flag_minus_max_cume_dist,
        DENSE_RANK() OVER (PARTITION BY is_skipped_flag, source_len ORDER BY is_skipped_flag_minus_max NULLS LAST, id) AS is_skipped_flag_minus_max_dense_rank,
        ROUND(max_is_skipped_flag / NULLIF(SUM(max_is_skipped_flag) OVER (PARTITION BY source_len), 0), 4) AS max_is_skipped_flag_share_of_source_len,
        ROW_NUMBER() OVER (PARTITION BY is_skipped_flag, source ORDER BY is_skipped_flag, id) AS rn,
        LAG(is_skipped_flag) OVER w_is_skipped_flag_source_len_b AS prev_is_skipped_flag,
        ROUND(source_len / NULLIF(SUM(source_len) OVER (PARTITION BY source_len), 0), 4) AS source_len_share_of_source_len
    FROM above_average
    WINDOW w_is_skipped_flag_source_len_b AS (PARTITION BY is_skipped_flag, source_len ORDER BY is_skipped_flag_minus_max NULLS LAST, id)
)
-- Final projection
SELECT
    id,
    listener_id,
    device,
    is_skipped,
    source,
    is_skipped_flag,
    ms_played_bucket,
    device_clean,
    is_skipped_flag_minus_max,
    is_skipped_flag_minus_max_cume_dist,
    is_skipped_flag_minus_max_dense_rank,
    max_is_skipped_flag_share_of_source_len,
    rn,
    prev_is_skipped_flag,
    source_len_share_of_source_len
FROM ranked
