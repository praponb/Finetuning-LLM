WITH filtered_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM plays
    WHERE listener_id IS NOT NULL
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at,
        UPPER(TRIM(device)) AS device_clean
    FROM filtered_plays
    WHERE ms_played_filled BETWEEN 44500 AND 380250
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        is_skipped,
        COUNT(*) AS row_count,
        SUM(ms_played) FILTER (WHERE NOT is_skipped) AS ms_played_not_is_skipped,
        MAX(played_at) AS last_played_at,
        SUM(days_since_played_at) AS total_days_since_played_at
    FROM enriched
    GROUP BY is_skipped
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        is_skipped,
        row_count,
        ms_played_not_is_skipped,
        last_played_at,
        DENSE_RANK() OVER (ORDER BY row_count) AS row_count_dense_rank,
        MIN(total_days_since_played_at) OVER (PARTITION BY is_skipped, row_count) AS is_skipped_row_count_min_total_days_since_played
    FROM stats
)
SELECT is_skipped, metric_name, reading
FROM (
    SELECT is_skipped, row_count, is_skipped_row_count_min_total_days_since_played, row_count_dense_rank, ms_played_not_is_skipped
    FROM ranked
) src
UNPIVOT (
    reading FOR metric_name IN (row_count, is_skipped_row_count_min_total_days_since_played, row_count_dense_rank, ms_played_not_is_skipped)
) unp
