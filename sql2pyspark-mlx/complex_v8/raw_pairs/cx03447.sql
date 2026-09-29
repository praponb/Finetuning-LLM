WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source,
        DATEDIFF(DATE '2025-12-31', played_at) AS days_since_played_at,
        LENGTH(TRIM(source)) AS source_len
    FROM plays
    WHERE is_skipped = TRUE
),
with_details_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.source,
        s.days_since_played_at,
        s.source_len,
        l.username AS listener_username
    FROM valid s
    JOIN listeners l ON l.id = s.listener_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        played_at,
        device,
        source,
        days_since_played_at,
        source_len,
        listener_username,
        SUM(days_since_played_at) OVER (PARTITION BY track_id ORDER BY played_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS days_since_played_at_sum_2_prior,
        LEAD(source_len, 3, 0) OVER (ORDER BY source_len, id) AS next3_source_len,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER w_track_id_by_played_at)) / 60, 1) AS minutes_since_prev_played_at,
        ROUND(source_len - AVG(source_len) OVER (PARTITION BY source_len, listener_id), 2) AS source_len_vs_source_len_listener_id_avg,
        COUNT(*) OVER (PARTITION BY track_id, source) AS rows_in_track_id_source,
        NTH_VALUE(source_len, 2) OVER (PARTITION BY source_len ORDER BY source_len ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_source_len,
        ROUND(AVG(source_len) OVER (ORDER BY played_at DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS source_len_avg_3_prior,
        ROUND(AVG(source_len) OVER (PARTITION BY source_len, listener_id), 2) AS source_len_listener_id_avg_source_len,
        COUNT(*) OVER (PARTITION BY track_id ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM with_details_listener
    WINDOW w_track_id_by_played_at AS (PARTITION BY track_id ORDER BY played_at DESC, id)
)
-- Final projection
SELECT
    id,
    played_at,
    device,
    source,
    days_since_played_at,
    listener_username,
    days_since_played_at_sum_2_prior,
    next3_source_len,
    source_len_vs_source_len_listener_id_avg,
    second_source_len,
    source_len_listener_id_avg_source_len,
    COALESCE(days_since_played_at, 0) AS days_since_played_at_filled
FROM with_windows
