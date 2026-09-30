WITH prepared_tracks AS (
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
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        ABS(bpm - 100) AS bpm_distance
    FROM tracks
    WHERE release_date BETWEEN DATE '2024-05-12' AND DATE '2025-04-02'
      AND artist_id IS NOT NULL
),
flattened AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.release_date_month,
        s.bpm_distance,
        t.mood
    FROM prepared_tracks s
    LATERAL VIEW EXPLODE(s.moods) t AS mood
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, mood, measure, value
FROM (
    SELECT id, mood, bpm_distance, bpm
    FROM flattened
) src
UNPIVOT INCLUDE NULLS (
    value FOR measure IN (bpm_distance AS bpm_distance_v, bpm AS m_bpm)
) unp
ORDER BY id, mood, measure
