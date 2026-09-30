WITH recent AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        duration_sec + bpm AS duration_sec_plus_bpm,
        ARRAY_CONTAINS(moods, 'focus') AS has_focus
    FROM tracks
    WHERE duration_sec <= 357
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.bpm,
        s.moods,
        s.has_focus,
        a.country AS artist_country,
        a.genre AS artist_genre
    FROM recent s
    LEFT JOIN artists a ON a.id = s.artist_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        title,
        moods,
        has_focus,
        artist_country,
        artist_genre,
        ROUND(AVG(duration_sec) OVER w_artist_genre, 2) AS artist_genre_avg_duration_sec,
        MAX(duration_sec) OVER (ORDER BY bpm, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS duration_sec_max_2,
        NTILE(5) OVER (ORDER BY duration_sec ASC, id) AS duration_sec_quintile,
        ROUND(CUME_DIST() OVER w_all_by_duration_sec_ties, 3) AS duration_sec_cume_dist,
        ROUND(CUME_DIST() OVER w_artist_country_artist_genre, 3) AS bpm_cume_dist,
        MAX(bpm) OVER (PARTITION BY artist_genre, has_focus) AS artist_genre_has_focus_max_bpm,
        NTILE(4) OVER (ORDER BY bpm DESC, id) AS bpm_quartile,
        COUNT(*) OVER (PARTITION BY artist_genre, has_focus) AS rows_in_artist_genre_has_focus
    FROM with_parent
    WINDOW w_artist_genre AS (PARTITION BY artist_genre), w_all_by_duration_sec_ties AS (ORDER BY duration_sec ASC), w_artist_country_artist_genre AS (PARTITION BY artist_country, artist_genre ORDER BY bpm, id)
)
SELECT
    id,
    title,
    moods,
    has_focus,
    artist_country,
    artist_genre,
    artist_genre_avg_duration_sec,
    duration_sec_max_2,
    duration_sec_quintile,
    duration_sec_cume_dist,
    bpm_cume_dist,
    bpm_quartile,
    rows_in_artist_genre_has_focus
FROM with_windows
ORDER BY title DESC
