WITH base AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        DATEDIFF(DATE '2025-12-31', played_at) AS days_since_played_at
    FROM plays
    WHERE TO_DATE(played_at) < DATE '2025-03-08'
),
joined_track AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.days_since_played_at,
        t.duration_sec AS track_duration_sec,
        t.is_explicit AS track_is_explicit
    FROM base s
    JOIN tracks t ON t.id = s.track_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY track_is_explicit, track_id ORDER BY played_at DESC NULLS FIRST, id) AS seq_no,
        track_duration_sec - LAG(track_duration_sec) OVER (ORDER BY days_since_played_at, id) AS track_duration_sec_change,
        ROUND(PERCENT_RANK() OVER (PARTITION BY track_is_explicit, track_id ORDER BY played_at DESC NULLS FIRST, id), 3) AS played_at_percent_rank,
        ROUND(AVG(days_since_played_at) OVER w_is_skipped_track_id, 2) AS is_skipped_track_id_avg_days_since_played_at,
        ROW_NUMBER() OVER w_all_by_days_since_played_at AS seq_no_2,
        LAST_VALUE(played_at) OVER (PARTITION BY track_is_explicit, track_id ORDER BY played_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at,
        SUM(days_since_played_at) OVER (PARTITION BY is_skipped, track_id) AS is_skipped_track_id_sum_days_since_played_at,
        ROUND(AVG(days_since_played_at) OVER (ORDER BY played_at ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS days_since_played_at_avg_3,
        COUNT(*) OVER (PARTITION BY is_skipped, track_id) AS rows_in_is_skipped_track_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY track_is_explicit, track_id ORDER BY played_at DESC NULLS FIRST), 3) AS played_at_percent_rank_2
    FROM joined_track
    WINDOW w_is_skipped_track_id AS (PARTITION BY is_skipped, track_id), w_all_by_days_since_played_at AS (ORDER BY days_since_played_at, id)
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        track_id,
        played_at,
        device,
        is_skipped,
        days_since_played_at,
        track_duration_sec,
        track_is_explicit,
        track_duration_sec_change,
        played_at_percent_rank,
        last_played_at,
        is_skipped_track_id_sum_days_since_played_at,
        days_since_played_at_avg_3,
        rows_in_is_skipped_track_id,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag,
        ROUND(played_at_percent_rank * 1.1, 2) AS played_at_percent_rank_adj
    FROM with_windows
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_skipped ORDER BY played_at), 3) AS played_at_percent_rank_2,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', last_played_at) ORDER BY rows_in_is_skipped_track_id DESC, id) AS rows_in_is_skipped_track_id_dense_rank,
        NTH_VALUE(track_duration_sec, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', last_played_at) ORDER BY rows_in_is_skipped_track_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_track_duration_sec
    FROM tagged
)
SELECT
    id,
    track_id,
    played_at,
    device,
    is_skipped,
    days_since_played_at,
    track_duration_sec,
    track_is_explicit,
    track_duration_sec_change,
    played_at_percent_rank,
    last_played_at,
    is_skipped_track_id_sum_days_since_played_at,
    rows_in_is_skipped_track_id,
    is_skipped_flag,
    played_at_percent_rank_adj,
    played_at_percent_rank_2,
    rows_in_is_skipped_track_id_dense_rank,
    SUBSTRING(TRIM(device), 1, 3) AS device_prefix,
    IF(is_skipped, 'yes', 'no') AS is_skipped_label
FROM metrics
