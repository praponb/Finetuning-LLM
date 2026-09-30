WITH base_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_label,
        DATE_TRUNC('MONTH', release_date) AS release_date_month
    FROM tracks
    WHERE artist_id <= 10
),
latest_per_key AS (
    -- Keep the earliest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        title_slug,
        release_date_label,
        release_date_month
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY bpm, id) AS rn
        FROM base_tracks
    ) ranked
    WHERE rn = 1
),
with_counts AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_count,
        (SELECT MAX(t.release_date) FROM tracks t WHERE t.artist_id = s.artist_id) AS last_track_release_date,
        (SELECT MAX(t.release_date) FROM tracks t WHERE t.artist_id = s.artist_id) AS last_track_release_date_2
    FROM latest_per_key s
),
latest_per_key_2 AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        title_slug,
        release_date_label,
        release_date_month,
        track_count,
        last_track_release_date,
        last_track_release_date_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY track_count DESC, id DESC) AS rn
        FROM with_counts
    ) ranked
    WHERE rn = 1
),
with_parent AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        s.title_slug,
        s.release_date_label,
        s.track_count,
        s.last_track_release_date_2,
        a.is_verified AS artist_is_verified
    FROM latest_per_key_2 s
    JOIN artists a ON a.id = s.artist_id AND a.genre = 'Pop'
),
deduped AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        title_slug,
        release_date_label,
        track_count,
        last_track_release_date_2,
        artist_is_verified
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY track_count DESC, id DESC) AS rn
        FROM with_parent
    ) ranked
    WHERE rn = 1
),
artist_id_stats AS (
    -- Avg track_count per artist_id
    SELECT artist_id, AVG(track_count) AS avg_track_count
    FROM deduped
    GROUP BY artist_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_track_count,
        ROUND(s.track_count - g.avg_track_count, 2) AS track_count_minus_avg
    FROM deduped s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.track_count > g.avg_track_count
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, value
FROM (
    SELECT id, duration_sec, track_count, bpm
    FROM above_average
) src
UNPIVOT (
    value FOR metric_name IN (duration_sec, track_count, bpm)
) unp
