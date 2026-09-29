WITH scoped_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        DATE_TRUNC('QUARTER', release_date) AS release_date_quarter,
        CASE WHEN bpm >= 142 THEN 'HIGH' WHEN bpm >= 85 THEN 'MEDIUM' ELSE 'LOW' END AS bpm_tier
    FROM tracks
    WHERE LENGTH(TRIM(title)) >= 4
      AND SIZE(moods) >= 1
),
enriched_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.moods,
        s.release_date_quarter,
        a.country AS artist_country,
        a.is_verified AS artist_is_verified,
        a.genre AS artist_genre
    FROM scoped_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
),
summarized_rows AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE t.is_explicit = TRUE) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_is_explicit_count,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_count
    FROM enriched_artist s
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        moods,
        artist_country,
        artist_is_verified,
        track_is_explicit_count,
        track_count,
        ROUND(track_count / NULLIF(track_is_explicit_count, 0), 3) AS track_count_per_track_is_explicit_count
    FROM summarized_rows
    WHERE LOWER(title) LIKE '%ni%'
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    release_date,
    moods,
    artist_country,
    artist_is_verified,
    track_is_explicit_count,
    LOWER(REGEXP_REPLACE(TRIM(artist_country), ' ', '_')) AS artist_country_slug,
    track_is_explicit_count - track_count AS track_is_explicit_count_minus_track_count
FROM enriched
WHERE artist_id <= 14
ORDER BY title, artist_id DESC NULLS FIRST, id
LIMIT 10
