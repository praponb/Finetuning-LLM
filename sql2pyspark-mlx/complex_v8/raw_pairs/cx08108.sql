WITH prepared AS (
    -- Filter plays
    SELECT *
    FROM plays
    WHERE is_skipped = TRUE
),
with_parent AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        t.title AS track_title,
        l.birth_year AS listener_birth_year
    FROM prepared s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN listeners l ON l.id = s.listener_id
    WHERE UPPER(TRIM(t.title)) LIKE 'E%'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY MONTH(played_at)), 2) AS ms_played_vs_played_at_month_avg,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY MONTH(played_at))) AS distinct_track_id_in_played_at_month,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_skipped ORDER BY played_at DESC), 3) AS played_at_percent_rank,
        ROUND(listener_birth_year / NULLIF(SUM(listener_birth_year) OVER (PARTITION BY device, track_id), 0), 4) AS listener_birth_year_share_of_device_track_id,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY YEAR(played_at), listener_id ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        COUNT(*) OVER (PARTITION BY is_skipped ORDER BY ms_played DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(listener_birth_year - AVG(listener_birth_year) OVER (PARTITION BY MONTH(played_at)), 2) AS listener_birth_year_vs_played_at_month_avg,
        COUNT(*) OVER (PARTITION BY YEAR(played_at), listener_id ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_2,
        MAX(listener_birth_year) OVER (PARTITION BY device, track_id) AS device_track_id_max_listener_birth_year,
        DENSE_RANK() OVER (PARTITION BY is_skipped ORDER BY played_at DESC) AS played_at_dense_rank,
        COUNT(*) OVER (PARTITION BY MONTH(played_at)) AS rows_in_played_at_month,
        RANK() OVER w_is_skipped_by_ms_played AS ms_played_rank,
        LEAD(played_at) OVER (PARTITION BY track_id, device ORDER BY listener_birth_year DESC, id) AS next_played_at,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY device, track_id)) AS distinct_listener_id_in_device_track_id,
        SUM(listener_birth_year) OVER (PARTITION BY device, track_id) AS device_track_id_sum_listener_birth_year
    FROM with_parent
    WINDOW w_is_skipped_by_ms_played AS (PARTITION BY is_skipped ORDER BY ms_played DESC NULLS FIRST, id)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(played_at_percent_rank) IGNORE NULLS OVER (ORDER BY running_count_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at_percent_rank,
        MAX(rows_in_played_at_month) OVER (ORDER BY ms_played, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rows_in_played_at_month_max_7,
        RANK() OVER w_all_by_ms_played_ties AS ms_played_rank_2,
        rows_in_played_at_month - LAG(rows_in_played_at_month) OVER (ORDER BY ms_played, id) AS rows_in_played_at_month_change,
        played_at_percent_rank - LAG(played_at_percent_rank) OVER (ORDER BY running_count_2 DESC, id) AS played_at_percent_rank_change,
        COUNT(*) OVER w_listener_id_track_id AS rows_in_listener_id_track_id,
        LEAD(second_played_at) OVER (ORDER BY ms_played, id) AS next_second_played_at
    FROM sequenced
    WINDOW w_all_by_ms_played_ties AS (ORDER BY ms_played), w_listener_id_track_id AS (PARTITION BY listener_id, track_id)
)
-- Final projection
SELECT
    id,
    listener_id,
    played_at,
    ms_played,
    device,
    is_skipped,
    track_title,
    listener_birth_year,
    ms_played_vs_played_at_month_avg,
    distinct_track_id_in_played_at_month,
    played_at_percent_rank,
    listener_birth_year_share_of_device_track_id,
    listener_birth_year_vs_played_at_month_avg,
    running_count_2,
    device_track_id_max_listener_birth_year,
    played_at_dense_rank,
    next_played_at,
    distinct_listener_id_in_device_track_id,
    last_known_played_at_percent_rank,
    rows_in_played_at_month_max_7,
    ms_played_rank_2,
    rows_in_played_at_month_change,
    played_at_percent_rank_change,
    rows_in_listener_id_track_id,
    COALESCE(device, 'UNKNOWN') AS device_or_unknown,
    DATE_TRUNC('QUARTER', played_at) AS played_at_quarter
FROM windowed
WHERE ms_played_vs_played_at_month_avg < 280000.0
ORDER BY distinct_listener_id_in_device_track_id, listener_birth_year_share_of_device_track_id, id
LIMIT 20
