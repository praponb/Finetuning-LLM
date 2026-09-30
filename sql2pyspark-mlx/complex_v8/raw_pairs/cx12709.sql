WITH scoped AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        moods,
        ADD_MONTHS(release_date, 12) AS release_date_plus_12m,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag,
        ABS(duration_sec - 352) AS duration_sec_distance
    FROM tracks
),
enriched_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.is_explicit,
        s.moods,
        s.duration_sec_distance,
        a.genre AS artist_genre
    FROM scoped s
    JOIN artists a ON a.id = s.artist_id
)
-- Subtotals with ROLLUP
SELECT
    artist_genre,
    is_explicit,
    artist_id,
    SUM(duration_sec_distance) FILTER (WHERE artist_genre IN ('Pop', 'HipHop', 'Jazz')) AS duration_sec_distance_artist_genre_in_pop_hiphop
FROM enriched_artist
GROUP BY ROLLUP(artist_genre, is_explicit, artist_id)
ORDER BY artist_genre NULLS LAST, is_explicit NULLS LAST, artist_id NULLS LAST
