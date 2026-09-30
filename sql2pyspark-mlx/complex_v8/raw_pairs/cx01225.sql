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
        INITCAP(TRIM(title)) AS title_clean,
        bpm + duration_sec AS bpm_plus_duration_sec
    FROM tracks
    WHERE artist_id IS NOT NULL
),
artist_id_stats AS (
    -- Avg duration_sec per artist_id
    SELECT artist_id, AVG(duration_sec) AS avg_duration_sec
    FROM filtered_tracks
    GROUP BY artist_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_duration_sec,
        ROUND(s.duration_sec - g.avg_duration_sec, 2) AS duration_sec_minus_avg
    FROM filtered_tracks s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.duration_sec > g.avg_duration_sec
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.title_clean,
        s.bpm_plus_duration_sec,
        s.avg_duration_sec,
        s.duration_sec_minus_avg,
        a.is_verified AS artist_is_verified,
        a.country AS artist_country,
        a.artist_name
    FROM above_average s
    JOIN artists a ON a.id = s.artist_id
)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, duration_sec_minus_avg, avg_duration_sec
    FROM with_parent
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric_name IN (duration_sec_minus_avg, avg_duration_sec)
) unp
