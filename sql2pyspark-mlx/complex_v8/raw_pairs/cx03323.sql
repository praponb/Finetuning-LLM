WITH scoped_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label,
        bpm + duration_sec AS bpm_plus_duration_sec
    FROM tracks
    WHERE YEAR(release_date) = 2024
),
joined AS (
    -- Attach artist attributes
    SELECT
        s.*,
        a.country AS artist_country,
        a.genre AS artist_genre,
        a.artist_name
    FROM scoped_tracks s
    JOIN artists a ON a.id = s.artist_id
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(artist_country, 'ALL') AS artist_country,
    is_explicit,
    PERCENTILE_APPROX(bpm_plus_duration_sec, 0.5) AS median_bpm_plus_duration_sec,
    SUM(CASE WHEN is_explicit = TRUE THEN bpm ELSE 0 END) AS bpm_is_explicit_true,
    ARRAY_SORT(COLLECT_SET(artist_genre)) AS artist_genre_list
FROM joined
GROUP BY GROUPING SETS ((artist_country, is_explicit), (artist_country), (is_explicit), ())
ORDER BY artist_country NULLS LAST, is_explicit NULLS LAST
