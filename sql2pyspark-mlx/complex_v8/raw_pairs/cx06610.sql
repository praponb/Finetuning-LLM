WITH scoped_tracks AS (
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
        ARRAY_JOIN(ARRAY_SORT(moods), ',') AS moods_text
    FROM tracks
    WHERE is_explicit = FALSE
      AND ARRAY_CONTAINS(moods, 'workout')
),
without_matches AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM scoped_tracks s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
        )
),
artist_id_stats AS (
    -- Median bpm per artist_id
    SELECT artist_id, PERCENTILE_APPROX(bpm, 0.5) AS median_bpm
    FROM without_matches
    GROUP BY artist_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_bpm, ROUND(s.bpm - g.median_bpm, 2) AS bpm_minus_median
    FROM without_matches s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm >= g.median_bpm
)
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    release_date,
    is_explicit,
    bpm,
    moods,
    median_bpm,
    bpm_minus_median,
    DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date
FROM outliers
WHERE title IS NOT NULL
