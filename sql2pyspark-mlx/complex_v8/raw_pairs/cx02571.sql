WITH base_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        CASE WHEN bpm >= 142 THEN 'HIGH' WHEN bpm >= 85 THEN 'MEDIUM' ELSE 'LOW' END AS bpm_tier,
        LOWER(TRIM(title)) AS title_clean,
        ABS(bpm - 96) AS bpm_distance
    FROM tracks
    WHERE (is_explicit = TRUE OR title IS NOT NULL)
),
annotated AS (
    -- Label and score rows
    SELECT *, SIZE(moods) AS moods_count
    FROM base_tracks
),
combined_details AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.moods,
        s.title_clean,
        a.country AS artist_country,
        a.genre AS artist_genre
    FROM annotated s
    LEFT JOIN artists a ON a.id = s.artist_id
)
SELECT
    COALESCE(artist_country, 'ALL') AS artist_country,
    artist_id,
    GROUPING_ID() AS gid,
    DATEDIFF(MAX(release_date), MIN(release_date)) AS release_date_span_days,
    PERCENTILE_APPROX(duration_sec, 0.5) AS median_duration_sec,
    MIN(duration_sec) AS min_duration_sec
FROM combined_details
GROUP BY GROUPING SETS ((artist_country, artist_id), (artist_country), (artist_id), ())
ORDER BY artist_country NULLS LAST, artist_id NULLS LAST
