WITH prepared_artists AS (
    -- Filter artists
    SELECT
        id,
        artist_name,
        genre,
        debut_year,
        is_verified,
        CASE WHEN debut_year >= 2020 THEN 'HIGH' WHEN debut_year >= 1997 THEN 'MEDIUM' ELSE 'LOW' END AS debut_year_tier
    FROM artists
    WHERE LENGTH(TRIM(artist_name)) >= 4
),
genre_stats AS (
    -- Avg debut_year per genre
    SELECT genre, AVG(debut_year) AS avg_debut_year
    FROM prepared_artists
    GROUP BY genre
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_debut_year,
        ROUND(s.debut_year - g.avg_debut_year, 2) AS debut_year_minus_avg
    FROM prepared_artists s
    JOIN genre_stats g ON g.genre = s.genre
    WHERE s.debut_year > g.avg_debut_year
),
with_counts AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT MAX(t.release_date) FROM tracks t WHERE t.artist_id = s.id) AS last_track_release_date
    FROM compared s
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        DATE_TRUNC('WEEK', last_track_release_date) AS last_track_release_date_week,
        CONCAT(debut_year_tier, '#', CAST(id AS STRING)) AS debut_year_tier_ref
    FROM with_counts
    WHERE debut_year_tier IN ('HIGH', 'LOW')
),
derived AS (
    -- Add calculated columns
    SELECT *, LOWER(REGEXP_REPLACE(TRIM(genre), ' ', '_')) AS genre_slug
    FROM scored
    WHERE debut_year_tier_ref IS NOT NULL
),
track_stats_2 AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        COUNT(CASE WHEN is_explicit = TRUE THEN 1 END) AS track_is_explicit_count,
        COUNT(CASE WHEN NOT is_explicit THEN 1 END) AS track_is_explicit_count_2,
        COUNT(*) FILTER (WHERE is_explicit = TRUE) AS track_is_explicit_count_3
    FROM tracks
    GROUP BY artist_id
),
with_stats AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        COALESCE(t.track_is_explicit_count, 0) AS track_is_explicit_count,
        COALESCE(t.track_is_explicit_count_2, 0) AS track_is_explicit_count_2,
        COALESCE(t.track_is_explicit_count_3, 0) AS track_is_explicit_count_3
    FROM derived s
    LEFT JOIN track_stats_2 t ON t.artist_id = s.id
),
track_stats_3 AS (
    -- Per-artist tracks metrics
    SELECT artist_id, COUNT(*) AS track_count, ROUND(AVG(bpm), 2) AS avg_track_bpm
    FROM tracks
    GROUP BY artist_id
),
with_stats_2 AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        COALESCE(t.track_count, 0) AS track_count,
        t.avg_track_bpm
    FROM with_stats s
    LEFT JOIN track_stats_3 t ON t.artist_id = s.id
)
SELECT
    id,
    artist_name,
    is_verified,
    debut_year_tier,
    avg_debut_year,
    debut_year_tier_ref,
    genre_slug,
    track_is_explicit_count,
    track_is_explicit_count_3,
    track_count,
    CASE WHEN is_verified THEN 1 ELSE 0 END AS is_verified_flag
FROM with_stats_2
WHERE YEAR(last_track_release_date) = 2024
ORDER BY track_count DESC, debut_year_tier
