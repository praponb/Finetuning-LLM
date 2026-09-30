WITH cleaned_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        duration_sec + bpm AS duration_sec_plus_bpm
    FROM tracks
    WHERE duration_sec <= 352
),
profiled AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT MIN(t.release_date) FROM tracks t WHERE t.artist_id = s.artist_id) AS first_track_release_date
    FROM cleaned_tracks s
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, value
FROM (
    SELECT id, bpm, duration_sec, duration_sec_plus_bpm
    FROM profiled
) src
UNPIVOT (
    value FOR field IN (bpm AS m_bpm, duration_sec AS m_duration_sec, duration_sec_plus_bpm AS m_duration_sec_plus_bpm)
) unp
