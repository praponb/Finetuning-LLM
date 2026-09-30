WITH cleaned_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source
    FROM plays
    WHERE track_id >= 13
      AND NOT is_skipped
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.track_id = s.track_id) AS first_played_at,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.track_id = s.track_id) AS first_played_at_2,
        (SELECT COUNT(*) FILTER (WHERE p.device = 'car') FROM plays p WHERE p.track_id = s.track_id) AS play_device_count
    FROM cleaned_plays s
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.source,
        s.play_device_count,
        l.interests AS listener_interests,
        t.duration_sec AS track_duration_sec
    FROM profiled s
    JOIN listeners l ON l.id = s.listener_id AND l.signup_date < DATE '2024-12-21'
    LEFT JOIN tracks t ON t.id = s.track_id
),
play_stats_2 AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        MIN(played_at) AS first_played_at,
        MAX(played_at) AS last_played_at
    FROM plays
    GROUP BY listener_id
),
profiled_2 AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.first_played_at,
        p.last_played_at
    FROM with_details s
    LEFT JOIN play_stats_2 p ON p.listener_id = s.listener_id
),
enriched_track AS (
    -- Attach track attributes
    SELECT
        s.*,
        t.moods AS track_moods,
        t.title AS track_title,
        t.artist_id AS track_artist_id
    FROM profiled_2 s
    LEFT JOIN tracks t ON t.id = s.track_id
),
track_artist_id_stats AS (
    -- Avg play_device_count per track_artist_id
    SELECT track_artist_id, AVG(play_device_count) AS avg_play_device_count
    FROM enriched_track
    GROUP BY track_artist_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_play_device_count,
        ROUND(s.play_device_count - g.avg_play_device_count, 2) AS play_device_count_minus_avg
    FROM enriched_track s
    JOIN track_artist_id_stats g ON g.track_artist_id = s.track_artist_id
    WHERE s.play_device_count > g.avg_play_device_count
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        track_id,
        COUNT(DISTINCT id) AS distinct_ids,
        MIN(played_at) AS first_played_at_2,
        SUM(CASE WHEN source IS NOT NULL THEN 1 ELSE 0 END) AS n_source_is_not_null,
        ROUND(SUM(ms_played) / NULLIF(SUM(track_duration_sec), 0), 3) AS ms_played_to_track_duration_sec_ratio,
        SUM(CASE WHEN LENGTH(TRIM(track_title)) >= 4 THEN 1 ELSE 0 END) AS n_length_trim_track_title_4
    FROM above_average
    WHERE play_device_count > 2
    GROUP BY track_id
    HAVING AVG(play_device_count_minus_avg) > -5.6
)
-- Final projection
SELECT
    track_id,
    distinct_ids,
    first_played_at_2,
    n_source_is_not_null
FROM stats
