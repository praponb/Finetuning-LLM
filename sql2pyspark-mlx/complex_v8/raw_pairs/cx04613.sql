WITH base_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE bpm >= 94
      AND release_date < DATE '2025-08-02'
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        bpm,
        moods,
        DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date
    FROM base_tracks
    WHERE is_explicit = FALSE
),
filtered AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM tagged s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND UPPER(TRIM(t.title)) LIKE 'M%'
              AND t.duration_sec <= 365
        )
),
summary AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        artist_id,
        MIN(days_since_release_date) AS min_days_since_release_date,
        COUNT(*) AS row_count,
        SUM(CASE WHEN UPPER(TRIM(title)) LIKE 'E%' THEN bpm ELSE 0 END) AS bpm_upper_trim_title_like_e,
        MAX(release_date) AS last_release_date,
        ARRAY_SORT(COLLECT_SET(title)) AS title_list
    FROM filtered
    GROUP BY DATE_TRUNC('MONTH', release_date), artist_id
),
with_children_tracks AS (
    -- One row per track
    SELECT
        s.release_date_month,
        s.artist_id,
        s.row_count,
        s.last_release_date,
        s.title_list,
        t.id AS track_id,
        t.moods AS track_moods,
        t.is_explicit AS track_is_explicit
    FROM summary s
    JOIN tracks t ON t.artist_id = s.artist_id AND t.is_explicit = FALSE
),
per_label AS (
    -- One row per title_list_item
    SELECT
        s.release_date_month,
        s.artist_id,
        s.row_count,
        s.last_release_date,
        s.track_id,
        s.track_moods,
        s.track_is_explicit,
        t.title_list_item
    FROM with_children_tracks s
    LATERAL VIEW OUTER EXPLODE(s.title_list) t AS title_list_item
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(last_release_date) IGNORE NULLS OVER (PARTITION BY artist_id, track_id ORDER BY row_count DESC, release_date_month, artist_id, track_id, title_list_item ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last_release_date,
        ROUND(row_count - AVG(row_count) OVER (PARTITION BY row_count), 2) AS row_count_vs_row_count_avg
    FROM per_label
)
SELECT
    release_date_month,
    artist_id,
    row_count,
    last_release_date,
    track_id,
    track_moods,
    track_is_explicit,
    title_list_item,
    last_known_last_release_date,
    row_count_vs_row_count_avg,
    CASE WHEN row_count >= 9 THEN 'HIGH' WHEN row_count >= 5 THEN 'MEDIUM' ELSE 'LOW' END AS row_count_tier
FROM with_trends
WHERE YEAR(last_release_date) = 2024
ORDER BY title_list_item DESC
