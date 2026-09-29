WITH valid AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
        CONCAT(title, '#', CAST(artist_id AS STRING)) AS title_ref
    FROM tracks
    WHERE bpm >= 73
),
artist_id_stats AS (
    -- Median bpm per artist_id
    SELECT artist_id, PERCENTILE_APPROX(bpm, 0.5) AS median_bpm
    FROM valid
    GROUP BY artist_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_bpm, ROUND(s.bpm - g.median_bpm, 2) AS bpm_minus_median
    FROM valid s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm >= g.median_bpm
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, duration_sec, bpm
    FROM outliers
) src
UNPIVOT (
    metric_value FOR metric_name IN (duration_sec, bpm)
) unp
ORDER BY id, metric_name
