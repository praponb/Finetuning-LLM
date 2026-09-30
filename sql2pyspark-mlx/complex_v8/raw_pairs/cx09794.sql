WITH valid_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label_2
    FROM plays
    WHERE NOT is_skipped
      AND ms_played < 220498
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY is_skipped_label)) AS distinct_listener_id_in_is_skipped_label,
        ms_played - LAG(ms_played) OVER (ORDER BY ms_played DESC, id) AS ms_played_change,
        COUNT(*) OVER (PARTITION BY is_skipped_label) AS rows_in_is_skipped_label,
        SUM(ms_played) OVER (PARTITION BY is_skipped_label) AS is_skipped_label_sum_ms_played,
        DENSE_RANK() OVER (PARTITION BY listener_id ORDER BY played_at DESC NULLS FIRST) AS played_at_dense_rank,
        LAST_VALUE(ms_played) OVER (ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played,
        RANK() OVER w_listener_id_by_played_at_tie AS played_at_rank,
        MIN(ms_played) OVER (PARTITION BY is_skipped_label) AS is_skipped_label_min_ms_played,
        ROUND(AVG(ms_played) OVER (PARTITION BY is_skipped_label), 2) AS is_skipped_label_avg_ms_played,
        LAG(played_at) OVER w_listener_id_by_played_at AS prev_played_at
    FROM valid_plays
    WINDOW w_listener_id_by_played_at_tie AS (PARTITION BY listener_id ORDER BY played_at DESC NULLS FIRST), w_listener_id_by_played_at AS (PARTITION BY listener_id ORDER BY played_at DESC NULLS FIRST, id)
)
SELECT
    id,
    played_at,
    ms_played,
    device,
    is_skipped_label,
    is_skipped_label_2,
    ms_played_change,
    rows_in_is_skipped_label,
    is_skipped_label_sum_ms_played,
    played_at_dense_rank,
    last_ms_played,
    played_at_rank,
    is_skipped_label_avg_ms_played,
    prev_played_at,
    ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 3600, 1) AS hours_prev_played_at_to_played_at,
    CONCAT(is_skipped_label, '#', CAST(ms_played AS STRING)) AS is_skipped_label_ref
FROM sequenced
WHERE is_skipped_label_min_ms_played < 2202250
ORDER BY rows_in_is_skipped_label, is_skipped_label_2, id
LIMIT 5
