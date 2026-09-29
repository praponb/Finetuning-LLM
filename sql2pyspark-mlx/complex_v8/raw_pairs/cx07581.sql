WITH prepared_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        ABS(duration_sec - 183) AS duration_sec_distance,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
    FROM tracks
    WHERE (SIZE(moods) >= 1 OR bpm <= 155)
),
artist_id_stats AS (
    -- Avg duration_sec_distance per artist_id
    SELECT artist_id, AVG(duration_sec_distance) AS avg_duration_sec_distance
    FROM prepared_tracks
    GROUP BY artist_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_duration_sec_distance,
        ROUND(s.duration_sec_distance - g.avg_duration_sec_distance, 2) AS duration_sec_distance_minus_avg
    FROM prepared_tracks s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.duration_sec_distance > g.avg_duration_sec_distance
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count_2
    FROM vs_group s
)
-- Subtotals with ROLLUP
SELECT
    artist_id,
    play_count,
    play_count_2,
    ARRAY_SORT(COLLECT_SET(title)) AS title_list,
    ROUND(AVG(play_count), 2) AS avg_play_count
FROM with_counts
GROUP BY ROLLUP(artist_id, play_count, play_count_2)
ORDER BY artist_id NULLS LAST, play_count NULLS LAST, play_count_2 NULLS LAST
