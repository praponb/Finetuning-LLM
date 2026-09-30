WITH recent_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        ABS(ms_played - 151112) AS ms_played_distance,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE (source = 'album' OR ms_played <= 261579)
),
combined_details AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.ms_played_distance,
        s.ms_played_bucket,
        t.artist_id AS track_artist_id
    FROM recent_plays s
    LEFT JOIN tracks t ON t.id = s.track_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        played_at,
        device,
        is_skipped,
        ms_played_distance,
        ms_played_bucket,
        track_artist_id,
        ROUND(AVG(ms_played_distance) OVER (PARTITION BY device, is_skipped ORDER BY ms_played_distance, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS ms_played_distance_avg_4,
        ROUND(AVG(ms_played_bucket) OVER (ORDER BY played_at ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS ms_played_bucket_avg_7,
        ROW_NUMBER() OVER w_all_by_played_at AS played_at_seq,
        LEAD(played_at) OVER w_played_at_month_by_ms_played AS next_played_at,
        NTILE(10) OVER (PARTITION BY device ORDER BY ms_played_bucket ASC NULLS LAST, id) AS ms_played_bucket_decile,
        ROUND(CUME_DIST() OVER (PARTITION BY device ORDER BY ms_played_bucket ASC NULLS LAST), 3) AS ms_played_bucket_cume_dist,
        COUNT(*) OVER (PARTITION BY track_artist_id) AS rows_in_track_artist_id,
        LAST_VALUE(played_at) IGNORE NULLS OVER (ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY listener_id)) AS distinct_track_id_in_listener_id
    FROM combined_details
    WINDOW w_all_by_played_at AS (ORDER BY played_at ASC, id), w_played_at_month_by_ms_played AS (PARTITION BY DATE_TRUNC('MONTH', played_at) ORDER BY ms_played_bucket, id)
)
SELECT
    id,
    played_at,
    device,
    ms_played_distance,
    ms_played_bucket,
    track_artist_id,
    ms_played_distance_avg_4,
    ms_played_bucket_avg_7,
    played_at_seq,
    next_played_at,
    ms_played_bucket_decile,
    ms_played_bucket_cume_dist,
    rows_in_track_artist_id,
    last_known_played_at,
    distinct_track_id_in_listener_id,
    COALESCE(played_at_seq, 0) AS played_at_seq_filled,
    COALESCE(ms_played_distance, 0) AS ms_played_distance_filled
FROM with_trends
ORDER BY id DESC NULLS FIRST
