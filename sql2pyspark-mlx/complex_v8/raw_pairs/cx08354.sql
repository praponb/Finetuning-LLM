WITH cleaned_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        TO_DATE(played_at) AS played_at_date,
        CASE WHEN device IN ('phone', 'speaker') THEN 'group_a' WHEN device = 'tv' THEN 'group_b' ELSE 'other' END AS device_group
    FROM plays
    WHERE ms_played <= 301149
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_device_group_by_played_at_da, 3) AS played_at_date_percent_rank,
        LEAD(ms_played) OVER (PARTITION BY listener_id, device_group ORDER BY ms_played DESC, id) AS next_ms_played,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY track_id, listener_id)) AS distinct_source_in_track_id_listener_id,
        LAG(played_at_date, 2) OVER (ORDER BY ms_played ASC, id) AS prev2_played_at_date,
        SIZE(COLLECT_SET(track_id) OVER w_listener_id) AS distinct_track_id_in_listener_id,
        LAST_VALUE(played_at_date) IGNORE NULLS OVER (PARTITION BY device_group ORDER BY played_at_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at_date,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', played_at_date), listener_id) AS rows_in_played_at_date_month_listener_id,
        MIN(ms_played) OVER (PARTITION BY track_id, listener_id) AS track_id_listener_id_min_ms_played,
        ms_played - LAG(ms_played) OVER w_listener_id_by_played_at_dat AS ms_played_change,
        SUM(ms_played) OVER (PARTITION BY device_group, is_skipped ORDER BY ms_played, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS ms_played_sum_3_prior,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY DATE_TRUNC('MONTH', played_at_date), listener_id)) AS distinct_track_id_in_played_at_date_month_listen,
        SIZE(COLLECT_SET(device_group) OVER (PARTITION BY track_id, listener_id)) AS distinct_device_group_in_track_id_listener_id,
        NTH_VALUE(played_at_date, 2) OVER (PARTITION BY device_group, is_skipped ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at_date,
        NTILE(3) OVER (PARTITION BY listener_id ORDER BY played_at_date, id) AS played_at_date_tercile,
        FIRST_VALUE(played_at_date) OVER (ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_played_at_date
    FROM cleaned_plays
    WINDOW w_device_group_by_played_at_da AS (PARTITION BY device_group ORDER BY played_at_date), w_listener_id AS (PARTITION BY listener_id), w_listener_id_by_played_at_dat AS (PARTITION BY listener_id ORDER BY played_at_date, id)
)
-- Final projection
SELECT
    id,
    track_id,
    ms_played,
    is_skipped,
    device_group,
    played_at_date_percent_rank,
    distinct_source_in_track_id_listener_id,
    prev2_played_at_date,
    distinct_track_id_in_listener_id,
    last_known_played_at_date,
    rows_in_played_at_date_month_listener_id,
    track_id_listener_id_min_ms_played,
    ms_played_change,
    ms_played_sum_3_prior,
    distinct_track_id_in_played_at_date_month_listen,
    second_played_at_date,
    first_played_at_date
FROM with_windows
