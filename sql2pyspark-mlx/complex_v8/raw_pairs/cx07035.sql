WITH valid AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        CASE WHEN bpm >= 155 THEN 'HIGH' WHEN bpm >= 82 THEN 'MEDIUM' ELSE 'LOW' END AS bpm_tier,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag,
        DATE_TRUNC('WEEK', release_date) AS release_date_week
    FROM tracks
    WHERE bpm <= 147
      AND artist_id IS NOT NULL
),
with_details_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.bpm_tier,
        s.release_date_week,
        a.artist_name,
        a.country AS artist_country,
        a.genre AS artist_genre
    FROM valid s
    LEFT JOIN artists a ON a.id = s.artist_id AND a.debut_year >= 1994
),
with_parent_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.is_explicit,
        s.bpm,
        s.release_date_week,
        s.artist_name,
        s.artist_country,
        s.artist_genre,
        a.genre AS artist_genre_2,
        a.debut_year AS artist_debut_year
    FROM with_details_artist s
    JOIN artists a ON a.id = s.artist_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, amount
FROM (
    SELECT id, bpm, artist_debut_year
    FROM with_parent_artist
) src
UNPIVOT INCLUDE NULLS (
    amount FOR measure IN (bpm, artist_debut_year)
) unp
