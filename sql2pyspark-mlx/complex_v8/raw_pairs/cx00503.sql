WITH cleaned_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        CASE WHEN bpm >= 147 THEN 'large' WHEN bpm >= 85 THEN 'medium' ELSE 'small' END AS bpm_tier,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM tracks
    WHERE duration_sec >= 239
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm_tier,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label,
        LEAST(duration_sec, duration_sec_filled) AS least_duration_sec_duration_sec_filled
    FROM cleaned_tracks
),
enriched_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.is_explicit,
        s.bpm_tier,
        s.release_date_label,
        s.least_duration_sec_duration_sec_filled,
        a.genre AS artist_genre
    FROM derived s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.genre IS NOT NULL
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY artist_id, artist_genre ORDER BY least_duration_sec_duration_sec_filled NULLS LAST) AS least_duration_sec_duration_sec_filled_rank,
        ROW_NUMBER() OVER (PARTITION BY artist_id, artist_genre ORDER BY least_duration_sec_duration_sec_filled NULLS LAST, id) AS least_duration_sec_duration_sec_filled_seq,
        ROUND(PERCENT_RANK() OVER (ORDER BY least_duration_sec_duration_sec_filled DESC), 3) AS least_duration_sec_duration_sec_filled_percent_r,
        RANK() OVER (PARTITION BY artist_id, artist_genre ORDER BY least_duration_sec_duration_sec_filled NULLS LAST, id) AS least_duration_sec_duration_sec_filled_rank_2,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id, artist_genre ORDER BY least_duration_sec_duration_sec_filled NULLS LAST, id), 3) AS least_duration_sec_duration_sec_filled_cume_dist
    FROM enriched_artist
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count,
        (SELECT SUM(p.ms_played) FROM plays p WHERE p.track_id = s.id) AS total_play_ms_played
    FROM ranked s
),
enriched AS (
    -- Derive helper columns
    SELECT *, COALESCE(total_play_ms_played, 0) AS total_play_ms_played_filled
    FROM with_stats
    WHERE least_duration_sec_duration_sec_filled_cume_dist > 0.2
),
derived_2 AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS(' / ', artist_genre, title) AS artist_genre_title_label,
        ROUND(total_play_ms_played / NULLIF(play_count, 0), 3) AS total_play_ms_played_per_play_count
    FROM enriched
)
SELECT
    is_explicit,
    play_count,
    artist_id,
    COUNT(*) AS row_count
FROM derived_2
GROUP BY CUBE(is_explicit, play_count, artist_id)
ORDER BY is_explicit NULLS LAST, play_count NULLS LAST, artist_id NULLS LAST
