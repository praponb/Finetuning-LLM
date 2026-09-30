WITH scoped AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source,
        MONTH(played_at) AS played_at_month
    FROM plays
    WHERE is_skipped = FALSE
),
combined_details_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.played_at_month,
        l.plan AS listener_plan,
        l.interests AS listener_interests,
        l.username AS listener_username
    FROM scoped s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        played_at_month - LAG(played_at_month) OVER (ORDER BY played_at DESC, id) AS played_at_month_change,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY TO_DATE(played_at), listener_id ORDER BY played_at_month DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        COUNT(*) OVER (PARTITION BY track_id) AS rows_in_track_id,
        NTILE(3) OVER w_device_by_played_at_month AS played_at_month_tercile,
        NTILE(10) OVER w_device_by_played_at_month AS played_at_month_decile,
        SUM(played_at_month) OVER (PARTITION BY track_id) AS track_id_sum_played_at_month,
        ROW_NUMBER() OVER w_played_at_month_by_played_at AS played_at_seq,
        MIN(played_at_month) OVER (ORDER BY played_at_month DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_played_at_month_so_far,
        ROUND(played_at_month / NULLIF(SUM(played_at_month) OVER (PARTITION BY device), 0), 4) AS played_at_month_share_of_device,
        MIN(played_at_month) OVER (PARTITION BY track_id) AS track_id_min_played_at_month,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(played_at), listener_id ORDER BY played_at_month DESC) AS played_at_month_dense_rank,
        MIN(played_at_month) OVER (PARTITION BY listener_plan) AS listener_plan_min_played_at_month
    FROM combined_details_listener
    WINDOW w_device_by_played_at_month AS (PARTITION BY device ORDER BY played_at_month DESC, id), w_played_at_month_by_played_at AS (PARTITION BY played_at_month ORDER BY played_at, id)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        played_at,
        device,
        played_at_month,
        listener_plan,
        listener_interests,
        listener_username,
        played_at_month_change,
        second_played_at,
        rows_in_track_id,
        track_id_sum_played_at_month,
        played_at_seq,
        min_played_at_month_so_far,
        track_id_min_played_at_month,
        played_at_month_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY played_at_month_tercile ORDER BY second_played_at DESC, id) AS second_played_at_seq,
        COUNT(*) OVER (PARTITION BY device) AS rows_in_device,
        FIRST_VALUE(played_at_month_tercile) IGNORE NULLS OVER (PARTITION BY listener_id, played_at_seq ORDER BY played_at_month_decile ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at_month_tercile
    FROM with_trends
)
-- Final projection
SELECT
    id,
    device,
    played_at_month,
    listener_plan,
    played_at_month_change,
    second_played_at,
    rows_in_track_id,
    played_at_seq,
    min_played_at_month_so_far,
    played_at_month_dense_rank,
    second_played_at_seq,
    rows_in_device,
    second_played_at_seq + played_at_month_change AS second_played_at_seq_plus_played_at_month_change,
    CASE WHEN played_at_month_change >= 6 THEN 'A' WHEN played_at_month_change >= -4 THEN 'B' ELSE 'C' END AS played_at_month_change_tier
FROM with_windows
ORDER BY rows_in_device DESC NULLS FIRST
