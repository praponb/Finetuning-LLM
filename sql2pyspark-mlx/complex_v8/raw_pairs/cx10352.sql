WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
    WHERE is_explicit = TRUE
),
grouped AS (
    -- Group-level totals
    SELECT
        artist_id,
        COUNT(*) AS cnt,
        ROUND(SUM(bpm) / NULLIF(SUM(title_len), 0), 3) AS bpm_to_title_len_ratio,
        SUM(CASE WHEN is_explicit = TRUE THEN title_len ELSE 0 END) AS title_len_is_explicit_true
    FROM prepared
    WHERE title_len > 4
    GROUP BY artist_id
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM grouped
    WHERE title_len_is_explicit_true > (SELECT AVG(title_len_is_explicit_true) FROM grouped)
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        MIN(release_date) AS first_track_release_date,
        COUNT(*) AS track_count
    FROM tracks
    GROUP BY artist_id
),
profiled AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        t.first_track_release_date,
        COALESCE(t.track_count, 0) AS track_count
    FROM screened s
    LEFT JOIN track_stats t ON t.artist_id = s.artist_id
)
SELECT artist_id, metric_name, metric_value
FROM (
    SELECT artist_id, title_len_is_explicit_true, cnt
    FROM profiled
) src
UNPIVOT (
    metric_value FOR metric_name IN (title_len_is_explicit_true AS m_title_len_is_explicit_true, cnt AS cnt_v)
) unp
ORDER BY artist_id, metric_name
