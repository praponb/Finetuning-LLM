WITH filtered_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_label
    FROM plays
    WHERE listener_id IS NOT NULL
      AND MONTH(played_at) IN (2, 3)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(played_at, 3) OVER w_listener_id_track_id_by_ms_p AS next3_played_at,
        DENSE_RANK() OVER (PARTITION BY track_id ORDER BY ms_played DESC, id) AS ms_played_dense_rank,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_id, DATE_TRUNC('MONTH', played_at)), 2) AS listener_id_played_at_month_avg_ms_played,
        SUM(ms_played) OVER (PARTITION BY track_id ORDER BY ms_played DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ms_played_sum_3,
        LAG(played_at) OVER (PARTITION BY listener_id, track_id ORDER BY played_at, id) AS prev_played_at,
        MIN(ms_played) OVER (PARTITION BY listener_id, DATE_TRUNC('MONTH', played_at)) AS listener_id_played_at_month_min_ms_played,
        ROW_NUMBER() OVER w_listener_id_track_id_by_ms_p_2 AS seq_no
    FROM filtered_plays
    WINDOW w_listener_id_track_id_by_ms_p AS (PARTITION BY listener_id, track_id ORDER BY ms_played NULLS LAST, id), w_listener_id_track_id_by_ms_p_2 AS (PARTITION BY listener_id, track_id ORDER BY ms_played DESC, id)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY YEAR(played_at)) AS rows_in_played_at_year,
        NTILE(10) OVER (ORDER BY played_at DESC, id) AS played_at_decile,
        ROUND(AVG(ms_played_dense_rank) OVER (PARTITION BY track_id ORDER BY next3_played_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS ms_played_dense_rank_avg_7
    FROM with_trends
)
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    played_at_label,
    next3_played_at,
    listener_id_played_at_month_avg_ms_played,
    ms_played_sum_3,
    prev_played_at,
    rows_in_played_at_year,
    played_at_decile,
    ms_played_dense_rank_avg_7,
    IF(is_skipped, 'yes', 'no') AS is_skipped_label,
    LEAST(seq_no, ms_played) AS least_seq_no_ms_played
FROM ranked
ORDER BY listener_id_played_at_month_avg_ms_played DESC, id
LIMIT 10
