WITH recent AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm,
        moods,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
    FROM tracks
    WHERE bpm >= 96
      AND title IS NOT NULL
),
with_details AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.bpm,
        s.title_slug,
        a.is_verified AS artist_is_verified,
        a.genre AS artist_genre,
        a.country AS artist_country
    FROM recent s
    LEFT JOIN artists a ON a.id = s.artist_id
),
latest AS (
    -- Keep the latest row per artist_country
    SELECT
        id,
        artist_id,
        bpm,
        title_slug,
        artist_is_verified,
        artist_genre,
        artist_country
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_country ORDER BY bpm DESC, id DESC) AS rn
        FROM with_details
    ) ranked
    WHERE rn = 1
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN p.source NOT IN ('search', 'playlist') THEN 1 END) FROM plays p WHERE p.track_id = s.id) AS play_source_count,
        (SELECT ROUND(AVG(p.ms_played), 2) FROM plays p WHERE p.track_id = s.id) AS avg_play_ms_played,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count
    FROM latest s
)
-- Subtotals with GROUPING SETS
SELECT
    play_count,
    artist_is_verified,
    artist_country,
    COUNT(DISTINCT artist_genre) AS distinct_artist_genres
FROM profiled
GROUP BY GROUPING SETS ((play_count, artist_is_verified, artist_country), (play_count), (artist_is_verified), ())
ORDER BY play_count NULLS LAST, artist_is_verified NULLS LAST, artist_country NULLS LAST
