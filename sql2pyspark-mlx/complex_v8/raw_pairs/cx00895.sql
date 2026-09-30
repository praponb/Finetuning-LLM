WITH filtered_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        bpm,
        moods
    FROM tracks
    WHERE release_date < DATE '2025-08-07'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(duration_sec) OVER (PARTITION BY artist_id) AS artist_id_min_duration_sec,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_id), 2) AS duration_sec_vs_artist_id_avg,
        FIRST_VALUE(duration_sec) OVER (PARTITION BY artist_id ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY artist_id) AS artist_id_sum_duration_sec,
        ROUND(PERCENT_RANK() OVER (ORDER BY duration_sec ASC), 3) AS duration_sec_percent_rank
    FROM filtered_tracks
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.bpm,
        s.artist_id_min_duration_sec,
        s.duration_sec_vs_artist_id_avg,
        s.first_duration_sec,
        s.duration_sec_percent_rank,
        a.genre AS artist_genre
    FROM with_windows s
    LEFT JOIN artists a ON a.id = s.artist_id AND a.debut_year BETWEEN 1991 AND 2006
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        bpm,
        duration_sec_vs_artist_id_avg,
        first_duration_sec,
        duration_sec_percent_rank,
        artist_genre,
        COALESCE(artist_genre, 'UNKNOWN') AS artist_genre_or_unknown
    FROM with_details
)
-- Final projection
SELECT
    id,
    duration_sec_vs_artist_id_avg,
    first_duration_sec,
    duration_sec_percent_rank,
    artist_genre_or_unknown,
    GREATEST(first_duration_sec, duration_sec) AS greatest_first_duration_sec_duration_sec,
    LOWER(REGEXP_REPLACE(TRIM(artist_genre_or_unknown), ' ', '_')) AS artist_genre_or_unknown_slug
FROM annotated
WHERE artist_id >= 9
ORDER BY greatest_first_duration_sec_duration_sec, artist_genre_or_unknown_slug DESC
