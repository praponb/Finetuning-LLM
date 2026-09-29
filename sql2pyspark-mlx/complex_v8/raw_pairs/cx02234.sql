WITH valid_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        CONCAT(title, '#', CAST(duration_sec AS STRING)) AS title_ref
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'party')
),
combined_details AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        a.artist_name
    FROM valid_tracks s
    JOIN artists a ON a.id = s.artist_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        artist_name,
        SUM(duration_sec) OVER (PARTITION BY artist_id ORDER BY bpm DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS duration_sec_sum_2,
        ROUND(duration_sec - AVG(duration_sec) OVER w_artist_id, 2) AS duration_sec_vs_artist_id_avg
    FROM combined_details
    WINDOW w_artist_id AS (PARTITION BY artist_id)
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE p.is_skipped = TRUE) FROM plays p WHERE p.track_id = s.id) AS play_is_skipped_count,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.id) AS last_played_at
    FROM sequenced s
)
SELECT
    play_is_skipped_count,
    play_count,
    is_explicit,
    ARRAY_SORT(COLLECT_SET(artist_name)) AS artist_name_list,
    PERCENTILE_APPROX(play_count, 0.5) AS median_play_count,
    MIN(last_played_at) AS first_last_played_at
FROM profiled
GROUP BY ROLLUP(play_is_skipped_count, play_count, is_explicit)
