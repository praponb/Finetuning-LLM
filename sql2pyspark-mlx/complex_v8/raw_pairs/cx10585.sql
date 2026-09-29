WITH prepared_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        DATE_TRUNC('QUARTER', release_date) AS release_date_quarter
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'S%'
),
without_matches AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM prepared_tracks s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.listener_id IS NOT NULL
              AND p.device = 'car'
        )
)
SELECT id, metric, value
FROM (
    SELECT id, duration_sec, bpm
    FROM without_matches
) src
UNPIVOT (
    value FOR metric IN (duration_sec, bpm)
) unp
ORDER BY id, metric
