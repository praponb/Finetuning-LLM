WITH filtered_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
),
screened AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM filtered_tracks s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.release_date BETWEEN DATE '2024-08-26' AND DATE '2025-08-28'
              AND t.duration_sec > 184
        )
)
-- Subtotals with ROLLUP
SELECT
    artist_id,
    is_explicit,
    ROUND(SUM(duration_sec) / NULLIF(SUM(bpm), 0), 3) AS duration_sec_to_bpm_ratio,
    MAX(bpm) AS max_bpm
FROM screened
GROUP BY ROLLUP(artist_id, is_explicit)
