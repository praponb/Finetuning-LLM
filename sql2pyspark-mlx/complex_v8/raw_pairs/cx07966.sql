WITH prepared_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped
    FROM plays
    WHERE ms_played BETWEEN 112786 AND 236512
),
totals AS (
    -- Group-level totals
    SELECT
        device,
        TO_DATE(played_at) AS played_at_day,
        is_skipped,
        MAX(played_at) AS last_played_at,
        PERCENTILE_APPROX(ms_played, 0.5) AS median_ms_played,
        MIN(ms_played) AS min_ms_played,
        COUNT(DISTINCT track_id) AS distinct_track_ids
    FROM prepared_plays
    GROUP BY device, TO_DATE(played_at), is_skipped
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM totals
    WHERE median_ms_played > (SELECT MIN(median_ms_played) FROM totals)
),
tagged AS (
    -- Add calculated columns
    SELECT
        device,
        played_at_day,
        is_skipped,
        last_played_at,
        median_ms_played,
        min_ms_played,
        distinct_track_ids,
        DATE_FORMAT(last_played_at, 'yyyy-MM-dd') AS last_played_at_label
    FROM qualified
    WHERE is_skipped = TRUE
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(distinct_track_ids) IGNORE NULLS OVER (PARTITION BY distinct_track_ids ORDER BY median_ms_played DESC, device, played_at_day, is_skipped ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_distinct_track_ids,
        ROW_NUMBER() OVER (PARTITION BY distinct_track_ids ORDER BY median_ms_played DESC, device, played_at_day, is_skipped) AS median_ms_played_seq,
        MAX(min_ms_played) OVER (PARTITION BY MONTH(played_at_day)) AS played_at_day_month_max_min_ms_played,
        FIRST_VALUE(median_ms_played) IGNORE NULLS OVER (ORDER BY min_ms_played, device, played_at_day, is_skipped ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_median_ms_played,
        LAST_VALUE(min_ms_played) IGNORE NULLS OVER (PARTITION BY distinct_track_ids ORDER BY median_ms_played DESC, device, played_at_day, is_skipped ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_min_ms_played
    FROM tagged
),
top_rows AS (
    -- First row per group
    SELECT
        device,
        played_at_day,
        is_skipped,
        last_played_at,
        median_ms_played,
        min_ms_played,
        distinct_track_ids,
        last_played_at_label,
        last_known_distinct_track_ids,
        played_at_day_month_max_min_ms_played,
        first_known_median_ms_played,
        last_known_min_ms_played
    FROM sequenced
    WHERE median_ms_played_seq = 1
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY distinct_track_ids DESC) AS distinct_track_ids_dense_rank,
        LEAD(played_at_day, 3) OVER (ORDER BY distinct_track_ids DESC, device, played_at_day, is_skipped) AS next3_played_at_day,
        DENSE_RANK() OVER (PARTITION BY distinct_track_ids ORDER BY median_ms_played DESC) AS median_ms_played_dense_rank,
        NTH_VALUE(distinct_track_ids, 2) OVER (PARTITION BY distinct_track_ids ORDER BY median_ms_played DESC, device, played_at_day, is_skipped ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_distinct_track_ids,
        MIN(min_ms_played) OVER (PARTITION BY distinct_track_ids ORDER BY median_ms_played DESC, device, played_at_day, is_skipped ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_min_ms_played_so_far,
        SUM(last_known_min_ms_played) OVER (PARTITION BY distinct_track_ids) AS distinct_track_ids_sum_last_known_min_ms_played
    FROM top_rows
)
-- Final projection
SELECT
    device,
    played_at_day,
    is_skipped,
    last_played_at,
    median_ms_played,
    distinct_track_ids,
    last_played_at_label,
    last_known_distinct_track_ids,
    played_at_day_month_max_min_ms_played,
    first_known_median_ms_played,
    last_known_min_ms_played,
    distinct_track_ids_dense_rank,
    next3_played_at_day,
    median_ms_played_dense_rank,
    second_distinct_track_ids,
    min_min_ms_played_so_far,
    distinct_track_ids_sum_last_known_min_ms_played
FROM ranked
WHERE next3_played_at_day < DATE '2025-06-01'
ORDER BY last_known_min_ms_played DESC NULLS FIRST, last_played_at DESC
