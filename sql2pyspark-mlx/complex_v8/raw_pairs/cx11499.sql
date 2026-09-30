WITH scoped_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        CASE WHEN bpm >= 155 THEN 'large' WHEN bpm >= 96 THEN 'medium' ELSE 'small' END AS bpm_tier,
        COALESCE(bpm, 0) AS bpm_filled,
        DATEDIFF(DATE '2025-12-31', release_date) AS days_since_release_date
    FROM tracks
    WHERE NOT ARRAY_CONTAINS(moods, 'sad')
),
with_activity AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM scoped_tracks s
    WHERE s.artist_id IN (
            SELECT t.artist_id
            FROM tracks t
            WHERE t.bpm < 156
        )
),
joined_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        s.bpm_tier,
        s.bpm_filled,
        a.genre AS artist_genre,
        a.debut_year AS artist_debut_year,
        a.is_verified AS artist_is_verified
    FROM with_activity s
    JOIN artists a ON a.id = s.artist_id
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        duration_sec,
        bpm,
        bpm_tier,
        artist_genre,
        artist_debut_year,
        artist_is_verified,
        CONCAT(bpm_tier, '#', CAST(artist_debut_year AS STRING)) AS bpm_tier_ref
    FROM joined_artist
),
rollup_base AS (
    -- Summarise per group
    SELECT
        artist_is_verified,
        artist_genre,
        ROUND(SUM(bpm) / NULLIF(SUM(duration_sec), 0), 3) AS bpm_to_duration_sec_ratio,
        COUNT(DISTINCT artist_genre) AS distinct_artist_genres,
        ARRAY_SORT(COLLECT_SET(artist_genre)) AS artist_genre_list
    FROM enriched
    WHERE bpm <= 147
    GROUP BY artist_is_verified, artist_genre
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE bpm_to_duration_sec_ratio > (SELECT AVG(bpm_to_duration_sec_ratio) FROM rollup_base)
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM qualified
    WHERE bpm_to_duration_sec_ratio < (SELECT MAX(bpm_to_duration_sec_ratio) FROM qualified)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT distinct_artist_genres, artist_is_verified, artist_genre, bpm_to_duration_sec_ratio
    FROM without_matches
) src
PIVOT (
    SUM(bpm_to_duration_sec_ratio)
    FOR artist_genre IN ('Jazz', 'Classical', 'Indie')
)
ORDER BY distinct_artist_genres, artist_is_verified
