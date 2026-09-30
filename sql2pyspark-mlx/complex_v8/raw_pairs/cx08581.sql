WITH cleaned AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        SIZE(moods) AS moods_count,
        CONCAT(title, '#', CAST(bpm AS STRING)) AS title_ref,
        FLOOR(bpm / 25) * 25 AS bpm_bucket
    FROM tracks
    WHERE LENGTH(TRIM(title)) >= 6
      AND release_date BETWEEN DATE '2024-08-26' AND DATE '2025-03-29'
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        DAYOFWEEK(release_date) AS release_date_dayofweek,
        DATEDIFF(DATE '2025-06-30', release_date) AS days_since_release_date
    FROM cleaned
    WHERE artist_id <= 13
),
with_details_artist AS (
    -- Attach artist attributes
    SELECT
        s.*,
        a.genre AS artist_genre,
        a.debut_year AS artist_debut_year,
        a.country AS artist_country
    FROM tagged s
    JOIN artists a ON a.id = s.artist_id AND a.is_verified = FALSE
    WHERE a.genre = 'HipHop'
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        MAX(release_date) AS last_track_release_date,
        COUNT(*) FILTER (WHERE is_explicit = TRUE) AS track_is_explicit_count,
        MAX(release_date) AS last_track_release_date_2
    FROM tracks
    GROUP BY artist_id
),
with_counts AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        t.last_track_release_date,
        COALESCE(t.track_is_explicit_count, 0) AS track_is_explicit_count,
        t.last_track_release_date_2
    FROM with_details_artist s
    LEFT JOIN track_stats t ON t.artist_id = s.artist_id
)
SELECT
    track_is_explicit_count,
    artist_id,
    artist_country,
    GROUPING(track_is_explicit_count) AS is_total_track_is_explicit_count,
    DATEDIFF(MAX(last_track_release_date), MIN(last_track_release_date)) AS last_track_release_date_span_days,
    SUM(artist_debut_year) AS total_artist_debut_year,
    ROUND(STDDEV(bpm), 2) AS stddev_bpm
FROM with_counts
GROUP BY GROUPING SETS ((track_is_explicit_count, artist_id, artist_country), (track_is_explicit_count), (artist_id))
ORDER BY track_is_explicit_count NULLS LAST, artist_id NULLS LAST, artist_country NULLS LAST
