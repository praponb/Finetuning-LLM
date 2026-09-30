WITH prepared_plays AS (
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
    WHERE ms_played >= 71476
      AND source = 'playlist'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY source, device ORDER BY played_at ASC NULLS LAST, id) AS played_at_rank,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (PARTITION BY source, device ORDER BY played_at ASC NULLS LAST, id))) / 60, 1) AS minutes_since_prev_played_at
    FROM prepared_plays
),
totals AS (
    -- Summarise per group
    SELECT
        played_at_rank,
        device,
        SUM(played_at_rank) FILTER (WHERE is_skipped = FALSE) AS played_at_rank_is_skipped_false,
        COUNT(*) AS row_count,
        SUM(played_at_rank) AS total_played_at_rank,
        MAX(played_at) AS last_played_at,
        ROUND(SUM(minutes_since_prev_played_at) / NULLIF(SUM(played_at_rank), 0), 3) AS minutes_since_prev_played_at_to_played_at_rank_r,
        MIN(played_at) AS first_played_at
    FROM sequenced
    WHERE minutes_since_prev_played_at > 1000.0
    GROUP BY played_at_rank, device
    HAVING COUNT(*) >= 2
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT last_played_at, device, played_at_rank_is_skipped_false
    FROM totals
) src
PIVOT (
    MIN(played_at_rank_is_skipped_false)
    FOR device IN ('speaker', 'desktop', 'tv')
)
