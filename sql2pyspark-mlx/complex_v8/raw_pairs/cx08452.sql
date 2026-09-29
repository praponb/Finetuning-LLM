WITH filtered_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label_2,
        DATE_TRUNC('YEAR', release_date) AS release_date_year
    FROM tracks
),
summarized_rows AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_count
    FROM filtered_tracks s
)
-- Subtotals with CUBE
SELECT
    COALESCE(is_explicit_label_2, 'ALL') AS is_explicit_label_2,
    track_count,
    COALESCE(is_explicit_label, 'ALL') AS is_explicit_label,
    GROUPING_ID() AS gid,
    COUNT(*) AS n_rows,
    SUM(track_count) AS total_track_count
FROM summarized_rows
GROUP BY CUBE(is_explicit_label_2, track_count, is_explicit_label)
