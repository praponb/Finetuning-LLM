WITH valid_artists AS (
    -- Filter artists
    SELECT
        id,
        artist_name,
        genre,
        debut_year,
        is_verified,
        CASE WHEN debut_year >= 2006 THEN 'A' WHEN debut_year >= 1996 THEN 'B' ELSE 'C' END AS debut_year_tier,
        CASE WHEN is_verified THEN 1 ELSE 0 END AS is_verified_flag,
        CASE WHEN debut_year >= 2006 THEN 'HIGH' WHEN debut_year >= 1997 THEN 'MEDIUM' ELSE 'LOW' END AS debut_year_tier_2
    FROM artists
    WHERE LOWER(artist_name) LIKE '%ht%'
),
latest_per_key AS (
    -- Keep the earliest row per debut_year_tier, debut_year_tier_2
    SELECT
        id,
        artist_name,
        genre,
        debut_year,
        is_verified,
        debut_year_tier,
        is_verified_flag,
        debut_year_tier_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY debut_year_tier, debut_year_tier_2 ORDER BY is_verified_flag, id) AS rn
        FROM valid_artists
    ) ranked
    WHERE rn = 1
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM latest_per_key
    WHERE is_verified_flag > (SELECT AVG(is_verified_flag) FROM latest_per_key)
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        artist_name,
        debut_year,
        is_verified,
        is_verified_flag,
        debut_year_tier_2,
        CONCAT_WS(' / ', debut_year_tier, artist_name) AS debut_year_tier_artist_name_label
    FROM with_activity
)
-- Final projection
SELECT
    id,
    artist_name,
    debut_year,
    is_verified,
    is_verified_flag,
    debut_year_tier_artist_name_label,
    COALESCE(debut_year, 0) AS debut_year_filled
FROM labeled
WHERE is_verified_flag >= 0
ORDER BY debut_year_tier_artist_name_label
