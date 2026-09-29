WITH prepared AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        moods,
        DATEDIFF(DATE '2025-06-30', release_date) AS days_since_release_date
    FROM tracks
    WHERE release_date IS NOT NULL
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(bpm) IGNORE NULLS OVER (ORDER BY days_since_release_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm,
        LAST_VALUE(duration_sec) OVER (ORDER BY days_since_release_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_sec,
        SUM(bpm) OVER w_artist_id AS artist_id_sum_bpm
    FROM prepared
    WINDOW w_artist_id AS (PARTITION BY artist_id)
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.bpm,
        s.moods,
        s.days_since_release_date,
        s.first_known_bpm,
        s.last_duration_sec,
        s.artist_id_sum_bpm,
        a.genre AS artist_genre
    FROM with_trends s
    JOIN artists a ON a.id = s.artist_id AND a.genre = 'Jazz'
),
with_parent_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.bpm,
        s.moods,
        s.days_since_release_date,
        s.first_known_bpm,
        s.last_duration_sec,
        s.artist_genre,
        a.is_verified AS artist_is_verified,
        a.genre AS artist_genre_2
    FROM combined_details s
    JOIN artists a ON a.id = s.artist_id AND a.genre IS NOT NULL
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count_2
    FROM with_parent_artist s
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.bpm,
        s.days_since_release_date,
        s.last_duration_sec,
        s.artist_genre,
        s.artist_is_verified,
        s.artist_genre_2,
        s.play_count,
        s.play_count_2,
        a.is_verified AS artist_is_verified_2,
        a.country AS artist_country
    FROM profiled s
    JOIN artists a ON a.id = s.artist_id
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    days_since_release_date,
    last_duration_sec,
    artist_genre,
    artist_is_verified,
    play_count_2,
    artist_is_verified_2,
    artist_country,
    bpm - play_count AS bpm_minus_play_count,
    COALESCE(bpm, 0) AS bpm_filled
FROM with_parent
ORDER BY artist_country, artist_id DESC
