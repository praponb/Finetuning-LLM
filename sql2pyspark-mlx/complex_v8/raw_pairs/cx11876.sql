WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        ABS(ms_played - 130888) AS ms_played_distance
    FROM plays
    WHERE played_at < TIMESTAMP '2025-03-02 09:00:00'
      AND device = 'desktop'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY is_skipped ORDER BY played_at DESC, id) AS seq_no,
        NTILE(5) OVER (PARTITION BY listener_id ORDER BY ms_played_distance DESC, id) AS ms_played_distance_quintile,
        COUNT(*) OVER (PARTITION BY YEAR(played_at), is_skipped) AS rows_in_played_at_year_is_skipped,
        MAX(ms_played_distance) OVER (PARTITION BY listener_id ORDER BY ms_played_distance DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ms_played_distance_max_7,
        LEAD(played_at, 2) OVER w_track_id_played_at_to_date_b AS next2_played_at,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY YEAR(played_at), is_skipped)) AS distinct_listener_id_in_played_at_year_is_skippe,
        SIZE(COLLECT_SET(device) OVER (PARTITION BY YEAR(played_at), is_skipped)) AS distinct_device_in_played_at_year_is_skipped,
        ROUND(ms_played_distance / NULLIF(SUM(ms_played_distance) OVER (PARTITION BY track_id, device), 0), 4) AS ms_played_distance_share_of_track_id_device,
        MIN(ms_played_distance) OVER (PARTITION BY track_id, device) AS track_id_device_min_ms_played_distance,
        FIRST_VALUE(played_at) OVER (PARTITION BY track_id, TO_DATE(played_at) ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_played_at,
        DENSE_RANK() OVER w_track_id_played_at_to_date_b_3 AS played_at_dense_rank,
        DENSE_RANK() OVER (PARTITION BY is_skipped ORDER BY played_at DESC) AS played_at_dense_rank_2,
        MAX(ms_played_distance) OVER w_played_at_month_source AS played_at_month_source_max_ms_played_distance,
        RANK() OVER w_source_track_id_by_ms_played AS ms_played_distance_rank,
        LAG(played_at, 3) OVER (PARTITION BY source, track_id ORDER BY ms_played_distance ASC, id) AS prev3_played_at
    FROM prepared
    WINDOW w_track_id_played_at_to_date_b AS (PARTITION BY track_id, TO_DATE(played_at) ORDER BY played_at ASC, id), w_track_id_played_at_to_date_b_3 AS (PARTITION BY track_id, TO_DATE(played_at) ORDER BY played_at ASC), w_played_at_month_source AS (PARTITION BY DATE_TRUNC('MONTH', played_at), source), w_source_track_id_by_ms_played AS (PARTITION BY source, track_id ORDER BY ms_played_distance ASC)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY track_id ORDER BY ms_played_distance_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at,
        NTH_VALUE(ms_played_distance_max_7, 2) OVER (ORDER BY next2_played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played_distance_max_7,
        COUNT(*) OVER w_device AS rows_in_device,
        ROUND(seq_no / NULLIF(SUM(seq_no) OVER (PARTITION BY device), 0), 4) AS seq_no_share_of_device,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY device)) AS distinct_track_id_in_device
    FROM with_trends
    WINDOW w_device AS (PARTITION BY device)
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    device,
    is_skipped,
    source,
    ms_played_distance,
    seq_no,
    ms_played_distance_quintile,
    rows_in_played_at_year_is_skipped,
    ms_played_distance_max_7,
    next2_played_at,
    distinct_listener_id_in_played_at_year_is_skippe,
    ms_played_distance_share_of_track_id_device,
    first_played_at,
    played_at_dense_rank,
    played_at_dense_rank_2,
    played_at_month_source_max_ms_played_distance,
    ms_played_distance_rank,
    second_ms_played_distance_max_7,
    seq_no_share_of_device,
    CONCAT(source, '#', CAST(ms_played_distance_max_7 AS STRING)) AS source_ref
FROM sequenced
