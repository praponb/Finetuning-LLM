WITH filtered AS (
    -- Filter artists
    SELECT
        id,
        artist_name,
        country,
        debut_year,
        FLOOR(debut_year / 5) * 5 AS debut_year_bucket
    FROM artists
    WHERE NOT is_verified
      AND UPPER(TRIM(artist_name)) LIKE 'P%'
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(artist_name, '#', CAST(debut_year_bucket AS STRING)) AS artist_name_ref
    FROM filtered
),
filtered_2 AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
              AND t.duration_sec > 165
              AND t.release_date IS NOT NULL
        )
)
-- Final projection
SELECT
    id,
    country,
    debut_year,
    debut_year_bucket,
    artist_name_ref,
    CASE WHEN country IN ('TH', 'US') THEN 'group_a' WHEN country = 'JP' THEN 'group_b' ELSE 'other' END AS country_group
FROM filtered_2
WHERE artist_name_ref IS NOT NULL
ORDER BY country DESC, debut_year
