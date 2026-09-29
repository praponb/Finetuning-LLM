WITH recent_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        FLOOR(bpm / 25) * 25 AS bpm_bucket,
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
    FROM tracks
    WHERE is_explicit = FALSE
      AND duration_sec <= 352
),
joined_artist AS (
    -- Attach artist attributes
    SELECT s.*, a.is_verified AS artist_is_verified
    FROM recent_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        MAX(release_date) AS last_track_release_date,
        MIN(release_date) AS first_track_release_date,
        COUNT(*) AS track_count
    FROM tracks
    GROUP BY artist_id
),
with_counts AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        t.last_track_release_date,
        t.first_track_release_date,
        COALESCE(t.track_count, 0) AS track_count
    FROM joined_artist s
    LEFT JOIN track_stats t ON t.artist_id = s.artist_id
)
-- Final projection
SELECT
    id,
    is_explicit,
    bpm,
    bpm_bucket,
    title_slug,
    artist_is_verified,
    last_track_release_date,
    track_count
FROM with_counts
WHERE bpm_bucket > 96
ORDER BY title_slug, bpm
