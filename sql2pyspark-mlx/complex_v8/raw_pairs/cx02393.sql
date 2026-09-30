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
        moods,
        CONCAT(title, '#', CAST(bpm AS STRING)) AS title_ref,
        duration_sec - bpm AS duration_sec_minus_bpm,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM tracks
    WHERE SIZE(moods) >= 2
),
filtered AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM filtered_tracks s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.title IS NOT NULL
        )
)
SELECT id, field, amount
FROM (
    SELECT id, bpm, duration_sec_minus_bpm, duration_sec
    FROM filtered
) src
UNPIVOT (
    amount FOR field IN (bpm AS bpm_v, duration_sec_minus_bpm AS duration_sec_minus_bpm_v, duration_sec AS m_duration_sec)
) unp
ORDER BY id, field
