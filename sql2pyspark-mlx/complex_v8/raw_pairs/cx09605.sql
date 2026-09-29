WITH scoped AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        genre,
        debut_year,
        CONCAT_WS(': ', country, artist_name) AS country_artist_name_label,
        CASE WHEN is_verified THEN 1 ELSE 0 END AS is_verified_flag,
        INITCAP(TRIM(artist_name)) AS artist_name_clean
    FROM artists
    WHERE (debut_year < 2018 OR genre = 'Jazz')
),
with_activity AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM scoped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
              AND SIZE(t.moods) >= 2
              AND t.title IS NOT NULL
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, value
FROM (
    SELECT id, debut_year, is_verified_flag
    FROM with_activity
) src
UNPIVOT INCLUDE NULLS (
    value FOR field IN (debut_year, is_verified_flag)
) unp
