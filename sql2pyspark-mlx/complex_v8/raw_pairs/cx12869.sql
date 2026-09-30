WITH base AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        ABS(ms_played - 145720) AS ms_played_distance,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE ms_played BETWEEN 62294 AND 360286
),
with_details_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.ms_played_distance,
        s.ms_played_bucket,
        t.release_date AS track_release_date,
        t.moods AS track_moods,
        t.artist_id AS track_artist_id
    FROM base s
    LEFT JOIN tracks t ON t.id = s.track_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ms_played_bucket - LAG(ms_played_bucket) OVER (PARTITION BY track_id ORDER BY track_release_date DESC, id) AS ms_played_bucket_change,
        DENSE_RANK() OVER (PARTITION BY track_artist_id, listener_id ORDER BY ms_played_bucket ASC NULLS LAST, id) AS ms_played_bucket_dense_rank,
        COUNT(*) OVER (PARTITION BY device) AS rows_in_device,
        MAX(ms_played_bucket) OVER w_device AS device_max_ms_played_bucket,
        LAST_VALUE(track_release_date) IGNORE NULLS OVER (PARTITION BY track_id ORDER BY ms_played_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_track_release_date,
        ROW_NUMBER() OVER w_track_artist_id_listener_id AS ms_played_bucket_seq
    FROM with_details_track
    WINDOW w_device AS (PARTITION BY device), w_track_artist_id_listener_id AS (PARTITION BY track_artist_id, listener_id ORDER BY ms_played_bucket ASC NULLS LAST, id)
),
first_rows AS (
    -- Top 2 per group
    SELECT *
    FROM with_windows
    WHERE ms_played_bucket_seq <= 2
      AND track_release_date BETWEEN DATE '2024-05-12' AND DATE '2025-03-29'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        track_id,
        ms_played,
        device,
        ms_played_distance,
        ms_played_bucket,
        track_moods,
        track_artist_id,
        ms_played_bucket_change,
        ms_played_bucket_dense_rank,
        last_known_track_release_date,
        ms_played_bucket_seq,
        COUNT(*) OVER (PARTITION BY listener_id) AS rows_in_listener_id,
        SUM(ms_played_bucket_change) OVER w_listener_id AS listener_id_sum_ms_played_bucket_change,
        ROUND(AVG(ms_played_distance) OVER (PARTITION BY track_id ORDER BY rows_in_device DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS ms_played_distance_avg_3_prior,
        ROW_NUMBER() OVER w_track_release_date_year_by_m AS rn,
        MAX(rows_in_device) OVER (PARTITION BY track_id ORDER BY rows_in_device DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS rows_in_device_max_6_prior,
        NTH_VALUE(ms_played_bucket_change, 2) OVER (PARTITION BY track_id ORDER BY rows_in_device DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played_bucket_change
    FROM first_rows
    WINDOW w_listener_id AS (PARTITION BY listener_id), w_track_release_date_year_by_m AS (PARTITION BY YEAR(track_release_date) ORDER BY ms_played_bucket_seq, id)
)
-- Final projection
SELECT
    id,
    device,
    ms_played_distance,
    ms_played_bucket,
    track_moods,
    track_artist_id,
    ms_played_bucket_change,
    last_known_track_release_date,
    ms_played_bucket_seq,
    rows_in_listener_id,
    listener_id_sum_ms_played_bucket_change,
    rn,
    rows_in_device_max_6_prior,
    second_ms_played_bucket_change,
    CASE WHEN ms_played_bucket >= 281500 THEN 'large' WHEN ms_played_bucket >= 143250 THEN 'medium' ELSE 'small' END AS ms_played_bucket_tier,
    CASE WHEN device IN ('desktop', 'car') THEN 'group_a' WHEN device = 'phone' THEN 'group_b' ELSE 'other' END AS device_group
FROM sequenced
ORDER BY rn DESC, ms_played_bucket_seq
