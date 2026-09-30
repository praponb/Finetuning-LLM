WITH scoped_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        moods,
        LOWER(TRIM(title)) AS title_clean,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label,
        GREATEST(duration_sec, bpm) AS greatest_duration_sec_bpm
    FROM tracks
    WHERE artist_id >= 12
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(greatest_duration_sec_bpm) OVER (PARTITION BY is_explicit, artist_id), 2) AS is_explicit_artist_id_avg_greatest_duration_sec_,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_id ORDER BY duration_sec DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_3
    FROM scoped_tracks
),
profiled AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT MAX(t.release_date) FROM tracks t WHERE t.artist_id = s.artist_id) AS last_track_release_date,
        (SELECT MAX(t.bpm) FROM tracks t WHERE t.artist_id = s.artist_id) AS max_track_bpm
    FROM windowed s
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        release_date_label,
        greatest_duration_sec_bpm,
        duration_sec_avg_3,
        MAX(duration_sec) OVER (PARTITION BY artist_id, is_explicit) AS artist_id_is_explicit_max_duration_sec,
        DENSE_RANK() OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec_avg_3 DESC, id) AS duration_sec_avg_3_dense_rank,
        COUNT(*) OVER (PARTITION BY artist_id, is_explicit) AS rows_in_artist_id_is_explicit,
        DATEDIFF(last_track_release_date, LAG(last_track_release_date) OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec_avg_3 DESC, id)) AS days_since_prev_last_track_release_date
    FROM profiled
),
filtered AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM with_trends s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.release_date < DATE '2025-03-08'
              AND t.is_explicit = FALSE
        )
      AND s.duration_sec_avg_3 <= 271.5
)
-- Final projection
SELECT
    id,
    title,
    duration_sec,
    is_explicit,
    release_date_label,
    duration_sec_avg_3,
    artist_id_is_explicit_max_duration_sec,
    duration_sec_avg_3_dense_rank,
    rows_in_artist_id_is_explicit,
    days_since_prev_last_track_release_date,
    CASE WHEN duration_sec >= 352 THEN 'A' WHEN duration_sec >= 165 THEN 'B' ELSE 'C' END AS duration_sec_tier
FROM filtered
ORDER BY days_since_prev_last_track_release_date, id
LIMIT 20
