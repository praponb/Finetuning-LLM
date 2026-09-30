WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        QUARTER(release_date) AS release_date_quarter
    FROM tracks
    WHERE is_explicit = FALSE
      AND duration_sec >= 154
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.release_date_quarter,
        p.id AS play_id,
        p.source AS play_source,
        p.device AS play_device
    FROM cleaned s
    JOIN plays p ON p.track_id = s.id
),
filtered AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM with_children_plays s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND UPPER(TRIM(t.title)) LIKE 'E%'
              AND YEAR(t.release_date) = 2024
        )
)
SELECT id, play_id, metric_name, value
FROM (
    SELECT id, play_id, release_date_quarter, bpm
    FROM filtered
) src
UNPIVOT (
    value FOR metric_name IN (release_date_quarter, bpm)
) unp
ORDER BY id, play_id, metric_name
