WITH scoped AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label,
        DATE_FORMAT(played_at, 'MMM yyyy') AS played_at_label
    FROM plays
    WHERE track_id IS NOT NULL
      AND played_at >= TIMESTAMP '2025-01-19 06:00:00'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER w_is_skipped AS rows_in_is_skipped,
        FIRST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', played_at) ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at,
        RANK() OVER w_played_at_month_by_played_at_2 AS played_at_rank,
        MAX(ms_played) OVER w_is_skipped AS is_skipped_max_ms_played,
        LAST_VALUE(played_at) OVER (PARTITION BY is_skipped_label, track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at,
        SIZE(COLLECT_SET(is_skipped_label) OVER (PARTITION BY is_skipped)) AS distinct_is_skipped_label_in_is_skipped,
        ROUND(AVG(ms_played) OVER (PARTITION BY is_skipped), 2) AS is_skipped_avg_ms_played,
        LEAD(played_at) OVER w_played_at_month_by_played_at_3 AS next_played_at,
        ms_played - LAG(ms_played) OVER (PARTITION BY is_skipped_label, listener_id ORDER BY ms_played, id) AS ms_played_change
    FROM scoped
    WINDOW w_is_skipped AS (PARTITION BY is_skipped), w_played_at_month_by_played_at_2 AS (PARTITION BY DATE_TRUNC('MONTH', played_at) ORDER BY played_at), w_played_at_month_by_played_at_3 AS (PARTITION BY DATE_TRUNC('MONTH', played_at) ORDER BY played_at, id)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        is_skipped_label,
        played_at_label,
        rows_in_is_skipped,
        first_known_played_at,
        played_at_rank,
        last_played_at,
        distinct_is_skipped_label_in_is_skipped,
        is_skipped_avg_ms_played,
        next_played_at,
        LEAD(played_at_rank, 3) OVER (PARTITION BY MONTH(played_at) ORDER BY rows_in_is_skipped DESC, id) AS next3_played_at_rank,
        ROUND(AVG(rows_in_is_skipped) OVER (PARTITION BY played_at_rank), 2) AS played_at_rank_avg_rows_in_is_skipped,
        LAST_VALUE(next_played_at) IGNORE NULLS OVER (PARTITION BY played_at_rank ORDER BY distinct_is_skipped_label_in_is_skipped ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_played_at,
        LAST_VALUE(ms_played_change) OVER (ORDER BY is_skipped_avg_ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played_change,
        ROUND(is_skipped_max_ms_played - AVG(is_skipped_max_ms_played) OVER (PARTITION BY played_at_rank), 2) AS is_skipped_max_ms_played_vs_played_at_rank_avg,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY played_at_rank), 2) AS ms_played_vs_played_at_rank_avg,
        SUM(played_at_rank) OVER (PARTITION BY played_at_rank) AS played_at_rank_sum_played_at_rank
    FROM with_windows
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    ms_played,
    is_skipped,
    is_skipped_label,
    played_at_label,
    rows_in_is_skipped,
    first_known_played_at,
    played_at_rank,
    last_played_at,
    distinct_is_skipped_label_in_is_skipped,
    is_skipped_avg_ms_played,
    played_at_rank_avg_rows_in_is_skipped,
    last_known_next_played_at,
    is_skipped_max_ms_played_vs_played_at_rank_avg,
    ms_played_vs_played_at_rank_avg,
    played_at_rank_sum_played_at_rank,
    CONCAT(played_at_label, '#', CAST(rows_in_is_skipped AS STRING)) AS played_at_label_ref
FROM sequenced
ORDER BY last_played_at DESC, id
LIMIT 5
