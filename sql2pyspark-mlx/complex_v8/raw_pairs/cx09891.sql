WITH scoped AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        moods,
        CASE WHEN duration_sec >= 312 THEN 'A' WHEN duration_sec >= 154 THEN 'B' ELSE 'C' END AS duration_sec_tier
    FROM tracks
    WHERE release_date < DATE '2024-12-03'
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        CASE WHEN bpm >= 163 THEN 'A' WHEN bpm >= 94 THEN 'B' ELSE 'C' END AS bpm_tier
    FROM scoped
    WHERE ARRAY_CONTAINS(moods, 'sad')
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
        s.duration_sec_tier,
        s.bpm_tier,
        a.genre AS artist_genre,
        a.country AS artist_country,
        a.artist_name
    FROM scored s
    JOIN artists a ON a.id = s.artist_id AND a.is_verified = TRUE
),
with_parent AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.moods,
        s.duration_sec_tier,
        s.bpm_tier,
        s.artist_genre,
        s.artist_country,
        s.artist_name,
        a.artist_name AS artist_artist_name,
        a.country AS artist_country_2,
        a.genre AS artist_genre_2
    FROM with_parent_artist s
    JOIN artists a ON a.id = s.artist_id
),
enriched_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.moods,
        s.duration_sec_tier,
        s.bpm_tier,
        s.artist_genre,
        s.artist_country,
        s.artist_name,
        s.artist_artist_name,
        s.artist_country_2,
        a.country AS artist_country_3,
        a.genre AS artist_genre_3,
        a.artist_name AS artist_artist_name_2
    FROM with_parent s
    JOIN artists a ON a.id = s.artist_id
)
-- Subtotals with CUBE
SELECT
    artist_genre_3,
    artist_id,
    ROUND(100 * COUNT(CASE WHEN UPPER(TRIM(artist_artist_name)) LIKE 'S%' THEN 1 END) / COUNT(*), 1) AS pct_upper_trim_artist_artist_nam,
    ARRAY_SORT(COLLECT_SET(artist_country_2)) AS artist_country_2_list
FROM enriched_artist
GROUP BY CUBE(artist_genre_3, artist_id)
