WITH prepared_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm,
        CASE WHEN bpm >= 147 THEN 'HIGH' WHEN bpm >= 89 THEN 'MEDIUM' ELSE 'LOW' END AS bpm_tier,
        ARRAY_CONTAINS(moods, 'sad') AS has_sad
    FROM tracks
    WHERE YEAR(release_date) = 2024
      AND bpm BETWEEN 75 AND 166
),
with_details AS (
    -- Attach artist attributes
    SELECT s.*, a.is_verified AS artist_is_verified
    FROM prepared_tracks s
    JOIN artists a ON a.id = s.artist_id
),
with_details_2 AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.bpm,
        s.moods,
        s.duration_sec_per_bpm,
        s.bpm_tier,
        s.has_sad,
        s.artist_is_verified,
        a.is_verified AS artist_is_verified_2,
        a.debut_year AS artist_debut_year,
        a.country AS artist_country
    FROM with_details s
    LEFT JOIN artists a ON a.id = s.artist_id AND a.country NOT IN ('BR', 'KR')
),
tagged AS (
    -- Add calculated columns
    SELECT *, SUBSTRING(TRIM(bpm_tier), 1, 3) AS bpm_tier_prefix
    FROM with_details_2
    WHERE duration_sec_per_bpm >= 1.0
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        MIN(duration_sec) AS min_track_duration_sec,
        COUNT(*) AS track_count
    FROM tracks
    GROUP BY artist_id
),
with_counts AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        t.min_track_duration_sec,
        COALESCE(t.track_count, 0) AS track_count
    FROM tagged s
    LEFT JOIN track_stats t ON t.artist_id = s.artist_id
)
SELECT
    id,
    artist_id,
    duration_sec,
    bpm,
    duration_sec_per_bpm,
    artist_is_verified,
    artist_is_verified_2,
    artist_debut_year,
    artist_country,
    bpm_tier_prefix,
    min_track_duration_sec,
    track_count,
    ROUND(duration_sec / NULLIF(track_count, 0), 3) AS duration_sec_per_track_count,
    GREATEST(track_count, min_track_duration_sec) AS greatest_track_count_min_track_duration_sec
FROM with_counts
ORDER BY artist_debut_year DESC NULLS FIRST, id
LIMIT 10
