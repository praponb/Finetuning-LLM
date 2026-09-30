WITH prepared AS (
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
        LEAST(bpm, duration_sec) AS least_bpm_duration_sec
    FROM tracks
    WHERE (bpm >= 94 OR duration_sec BETWEEN 165 AND 352)
),
with_parent_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.moods,
        s.least_bpm_duration_sec,
        a.artist_name,
        a.country AS artist_country
    FROM prepared s
    JOIN artists a ON a.id = s.artist_id
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        FLOOR(least_bpm_duration_sec / 50) * 50 AS least_bpm_duration_sec_bucket,
        least_bpm_duration_sec - duration_sec AS least_bpm_duration_sec_minus_duration_sec
    FROM with_parent_artist
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT artist_id, COUNT(*) AS track_count
    FROM tracks
    GROUP BY artist_id
),
summarized_rows AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        COALESCE(t.track_count, 0) AS track_count
    FROM tagged s
    LEFT JOIN track_stats t ON t.artist_id = s.artist_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, reading
FROM (
    SELECT id, duration_sec, least_bpm_duration_sec_minus_duration_sec
    FROM summarized_rows
) src
UNPIVOT (
    reading FOR field IN (duration_sec AS m_duration_sec, least_bpm_duration_sec_minus_duration_sec AS m_least_bpm_duration_sec_minus_duration_sec)
) unp
