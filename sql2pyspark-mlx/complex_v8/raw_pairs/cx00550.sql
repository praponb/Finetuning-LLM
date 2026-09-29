WITH scoped AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped
    FROM plays
    WHERE listener_id IS NOT NULL
      AND source = 'search'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        COUNT(*) OVER w_listener_id_track_id AS rows_in_listener_id_track_id,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_id, track_id), 2) AS listener_id_track_id_avg_ms_played,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY is_skipped), 2) AS ms_played_vs_is_skipped_avg,
        NTILE(5) OVER (PARTITION BY device, listener_id ORDER BY ms_played, id) AS ms_played_quintile,
        ROUND(AVG(ms_played) OVER (ORDER BY ms_played, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_2,
        NTILE(3) OVER (ORDER BY ms_played, id) AS ms_played_tercile,
        RANK() OVER (PARTITION BY is_skipped ORDER BY ms_played, id) AS ms_played_rank,
        ROUND(PERCENT_RANK() OVER w_is_skipped_by_ms_played, 3) AS ms_played_percent_rank,
        LEAD(played_at, 2) OVER w_all_by_ms_played AS next2_played_at
    FROM scoped
    WINDOW w_listener_id_track_id AS (PARTITION BY listener_id, track_id), w_is_skipped_by_ms_played AS (PARTITION BY is_skipped ORDER BY ms_played, id), w_all_by_ms_played AS (ORDER BY ms_played, id)
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN device IN ('phone', 'car') THEN 'group_a' WHEN device = 'desktop' THEN 'group_b' ELSE 'other' END AS device_group
    FROM windowed
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(ms_played) OVER (PARTITION BY track_id, ms_played_tercile), 2) AS track_id_ms_played_tercile_avg_ms_played,
        LEAD(next2_played_at, 2) OVER (ORDER BY ms_played_vs_is_skipped_avg, id) AS next2_next2_played_at,
        ROUND(CUME_DIST() OVER (PARTITION BY TO_DATE(played_at), device_group ORDER BY ms_played_rank), 3) AS ms_played_rank_cume_dist,
        NTILE(10) OVER (PARTITION BY TO_DATE(played_at), device_group ORDER BY ms_played_rank, id) AS ms_played_rank_decile,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY ms_played_quintile, is_skipped), 0), 4) AS ms_played_share_of_ms_played_quintile_is_skipped,
        RANK() OVER w_device_ms_played_tercile_by AS ms_played_avg_2_rank
    FROM labeled
    WINDOW w_device_ms_played_tercile_by AS (PARTITION BY device, ms_played_tercile ORDER BY ms_played_avg_2 DESC NULLS FIRST)
)
SELECT
    id,
    track_id,
    played_at,
    ms_played,
    device,
    is_skipped,
    rows_in_listener_id_track_id,
    listener_id_track_id_avg_ms_played,
    ms_played_vs_is_skipped_avg,
    ms_played_quintile,
    ms_played_avg_2,
    ms_played_tercile,
    ms_played_rank,
    next2_played_at,
    device_group,
    track_id_ms_played_tercile_avg_ms_played,
    next2_next2_played_at,
    ms_played_rank_cume_dist,
    ms_played_rank_decile,
    ms_played_share_of_ms_played_quintile_is_skipped,
    ms_played_avg_2_rank,
    SUBSTRING(TRIM(device), 1, 4) AS device_prefix
FROM sequenced
ORDER BY ms_played_tercile NULLS LAST
