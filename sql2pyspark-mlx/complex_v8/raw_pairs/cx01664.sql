WITH recent_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label_2
    FROM tracks
    WHERE NOT is_explicit
      AND artist_id >= 7
),
with_details AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.bpm,
        s.moods,
        s.release_date_label_2,
        a.artist_name
    FROM recent_tracks s
    JOIN artists a ON a.id = s.artist_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, amount
FROM (
    SELECT id, duration_sec, bpm
    FROM with_details
) src
UNPIVOT (
    amount FOR metric IN (duration_sec, bpm)
) unp
ORDER BY id, metric
