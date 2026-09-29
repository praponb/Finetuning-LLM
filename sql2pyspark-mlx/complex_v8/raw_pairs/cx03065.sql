WITH cleaned AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        DATE_TRUNC('WEEK', played_at) AS played_at_week
    FROM plays
    WHERE device NOT IN ('speaker', 'car')
      AND track_id IS NOT NULL
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER w_all_by_ms_played AS ms_played_dense_rank,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_listener_id_track_id, 0), 4) AS ms_played_share_of_listener_id_track_id,
        ROUND(CUME_DIST() OVER w_listener_id_track_id_by_ms_p, 3) AS ms_played_cume_dist,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY listener_id), 2) AS ms_played_vs_listener_id_avg,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_id, track_id), 2) AS listener_id_track_id_avg_ms_played,
        SUM(ms_played) OVER (ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played,
        FIRST_VALUE(ms_played) OVER (PARTITION BY listener_id, track_id ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played,
        ROUND(PERCENT_RANK() OVER (PARTITION BY listener_id, track_id ORDER BY ms_played DESC), 3) AS ms_played_percent_rank,
        COUNT(*) OVER (PARTITION BY listener_id) AS rows_in_listener_id,
        ROW_NUMBER() OVER (PARTITION BY track_id, DATE_TRUNC('MONTH', played_at) ORDER BY ms_played, id) AS seq_no
    FROM cleaned
    WINDOW w_all_by_ms_played AS (ORDER BY ms_played ASC, id), w_listener_id_track_id AS (PARTITION BY listener_id, track_id), w_listener_id_track_id_by_ms_p AS (PARTITION BY listener_id, track_id ORDER BY ms_played DESC)
),
top_per_group AS (
    -- Top 3 per group
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        played_at_week,
        ms_played_dense_rank,
        ms_played_share_of_listener_id_track_id,
        ms_played_cume_dist,
        ms_played_vs_listener_id_avg,
        listener_id_track_id_avg_ms_played,
        running_ms_played,
        first_ms_played,
        ms_played_percent_rank,
        rows_in_listener_id
    FROM metrics
    WHERE seq_no <= 3
)
SELECT
    id,
    track_id,
    played_at_week,
    ms_played_dense_rank,
    ms_played_share_of_listener_id_track_id,
    ms_played_cume_dist,
    ms_played_vs_listener_id_avg,
    listener_id_track_id_avg_ms_played,
    running_ms_played,
    first_ms_played,
    ms_played_percent_rank,
    rows_in_listener_id,
    CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag,
    FLOOR(ms_played_dense_rank / 2) * 2 AS ms_played_dense_rank_bucket
FROM top_per_group
