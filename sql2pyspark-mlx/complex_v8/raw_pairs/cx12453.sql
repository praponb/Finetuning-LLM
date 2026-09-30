WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label
    FROM plays
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER w_all_by_played_at_ties AS played_at_dense_rank,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY device, is_skipped_label ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        LAG(played_at) OVER (ORDER BY played_at DESC, id) AS prev_played_at,
        NTILE(4) OVER (PARTITION BY device, is_skipped_label ORDER BY played_at DESC, id) AS played_at_quartile,
        ROUND(CUME_DIST() OVER w_all_by_played_at_ties, 3) AS played_at_cume_dist,
        LAG(played_at) OVER (PARTITION BY device, is_skipped_label ORDER BY played_at DESC, id) AS prev_played_at_2,
        LEAD(played_at, 2) OVER (ORDER BY played_at, id) AS next2_played_at,
        ROUND(PERCENT_RANK() OVER w_device_by_played_at_ties, 3) AS played_at_percent_rank,
        NTH_VALUE(played_at, 2) OVER (ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at_2,
        ROW_NUMBER() OVER w_all_by_played_at_2 AS rn
    FROM cleaned
    WINDOW w_all_by_played_at_ties AS (ORDER BY played_at), w_device_by_played_at_ties AS (PARTITION BY device ORDER BY played_at), w_all_by_played_at_2 AS (ORDER BY played_at, id)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM ranked
    WHERE played_at_dense_rank = 1
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    is_skipped,
    is_skipped_label,
    played_at_dense_rank,
    second_played_at,
    prev_played_at,
    played_at_quartile,
    played_at_cume_dist,
    prev_played_at_2,
    played_at_percent_rank,
    second_played_at_2,
    rn,
    LOWER(TRIM(source)) AS source_clean
FROM leaders
