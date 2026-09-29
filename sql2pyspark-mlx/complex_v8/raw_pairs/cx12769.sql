WITH base AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        bpm,
        moods
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'G%'
      AND release_date BETWEEN DATE '2024-04-21' AND DATE '2025-08-07'
),
with_counts AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE NOT t.is_explicit) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_is_explicit_count,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_count,
        (SELECT MIN(t.release_date) FROM tracks t WHERE t.artist_id = s.artist_id) AS first_track_release_date
    FROM base s
)
-- Subtotals with CUBE
SELECT
    track_count,
    track_is_explicit_count,
    GROUPING_ID() AS gid,
    SUM(bpm) AS total_bpm,
    MIN(track_is_explicit_count) AS min_track_is_explicit_count
FROM with_counts
GROUP BY CUBE(track_count, track_is_explicit_count)
ORDER BY track_count NULLS LAST, track_is_explicit_count NULLS LAST
