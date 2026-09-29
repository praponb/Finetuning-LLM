WITH valid_artists AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        genre,
        is_verified,
        ABS(debut_year - 2006) AS debut_year_distance,
        CASE WHEN debut_year >= 2016 THEN 'A' WHEN debut_year >= 1996 THEN 'B' ELSE 'C' END AS debut_year_tier,
        CASE WHEN genre IN ('Pop', 'Indie') THEN 'group_a' WHEN genre = 'Classical' THEN 'group_b' ELSE 'other' END AS genre_group
    FROM artists
    WHERE LOWER(artist_name) LIKE '%gr%'
      AND genre IN ('Indie', 'Pop')
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM valid_artists
    WHERE debut_year_distance > (SELECT MIN(debut_year_distance) FROM valid_artists)
)
-- Subtotals with ROLLUP
SELECT
    genre_group,
    genre,
    is_verified,
    MIN(debut_year_distance) AS min_debut_year_distance,
    ARRAY_SORT(COLLECT_SET(genre)) AS genre_list
FROM with_activity
GROUP BY ROLLUP(genre_group, genre, is_verified)
ORDER BY genre_group NULLS LAST, genre NULLS LAST, is_verified NULLS LAST
