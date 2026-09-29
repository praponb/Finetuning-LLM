WITH prepared AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        CONCAT(title, '#', CAST(artist_id AS STRING)) AS title_ref
    FROM tracks
    WHERE duration_sec BETWEEN 164 AND 409
      AND bpm <= 163
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        COUNT(*) AS track_count,
        MIN(release_date) AS first_track_release_date
    FROM tracks
    GROUP BY artist_id
),
with_counts AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        COALESCE(t.track_count, 0) AS track_count,
        t.first_track_release_date
    FROM prepared s
    LEFT JOIN track_stats t ON t.artist_id = s.artist_id
),
artist_id_stats AS (
    -- Max bpm per artist_id
    SELECT artist_id, MAX(bpm) AS max_bpm
    FROM with_counts
    GROUP BY artist_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_bpm, ROUND(s.bpm - g.max_bpm, 2) AS bpm_minus_max
    FROM with_counts s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm = g.max_bpm
),
latest_per_key AS (
    -- Keep the earliest row per artist_id
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        title_ref,
        track_count,
        first_track_release_date,
        max_bpm,
        bpm_minus_max
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY first_track_release_date, id) AS recency_rank
        FROM outliers
    ) ranked
    WHERE recency_rank = 1
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN bpm_minus_max >= 54.0 THEN 'HIGH' WHEN bpm_minus_max >= -108.0 THEN 'MEDIUM' ELSE 'LOW' END AS bpm_minus_max_tier,
        ROUND(bpm_minus_max * 0.9, 2) AS bpm_minus_max_adj
    FROM latest_per_key
    WHERE first_track_release_date >= DATE '2024-07-01'
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    is_explicit,
    bpm,
    moods,
    track_count,
    first_track_release_date,
    max_bpm
FROM derived
