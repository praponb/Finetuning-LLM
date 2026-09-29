WITH valid AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        QUARTER(release_date) AS release_date_quarter
    FROM tracks
    WHERE (duration_sec BETWEEN 154 AND 409 OR ARRAY_CONTAINS(moods, 'party'))
),
artist_id_stats AS (
    -- Avg bpm per artist_id
    SELECT artist_id, AVG(bpm) AS avg_bpm
    FROM valid
    GROUP BY artist_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_bpm, ROUND(s.bpm - g.avg_bpm, 2) AS bpm_minus_avg
    FROM valid s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm > g.avg_bpm
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        is_explicit,
        release_date_quarter,
        artist_id,
        ARRAY_SORT(COLLECT_SET(title)) AS title_list,
        SUM(avg_bpm) AS total_avg_bpm,
        MIN(bpm_minus_avg) AS min_bpm_minus_avg,
        COUNT(*) AS cnt
    FROM outliers
    GROUP BY is_explicit, release_date_quarter, artist_id
),
filtered AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM grouped s
    WHERE s.artist_id IN (
            SELECT t.artist_id
            FROM tracks t
            WHERE t.bpm >= 100
              AND t.is_explicit = TRUE
        )
),
qualified AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND a.country = 'BR'
              AND LOWER(a.artist_name) LIKE '%un%'
        )
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT artist_id, MIN(release_date) AS first_track_release_date
    FROM tracks
    GROUP BY artist_id
),
with_stats AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        t.first_track_release_date
    FROM qualified s
    LEFT JOIN track_stats t ON t.artist_id = s.artist_id
),
track_stats_2 AS (
    -- Per-artist tracks metrics
    SELECT artist_id, MIN(bpm) AS min_track_bpm
    FROM tracks
    GROUP BY artist_id
),
with_counts AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        t.min_track_bpm
    FROM with_stats s
    LEFT JOIN track_stats_2 t ON t.artist_id = s.artist_id
)
SELECT
    is_explicit,
    release_date_quarter,
    artist_id,
    title_list,
    total_avg_bpm,
    min_bpm_minus_avg,
    first_track_release_date,
    FLOOR(cnt / 2) * 2 AS cnt_bucket,
    DATE_FORMAT(first_track_release_date, 'MMM yyyy') AS first_track_release_date_label
FROM with_counts
ORDER BY cnt_bucket DESC NULLS FIRST, release_date_quarter DESC
