WITH recent_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        QUARTER(release_date) AS release_date_quarter
    FROM tracks
),
rollup_base AS (
    -- Summarise per group
    SELECT
        is_explicit,
        artist_id,
        release_date_quarter,
        ROUND(STDDEV(release_date_quarter), 2) AS stddev_release_date_quarter,
        ROUND(SUM(release_date_quarter) / NULLIF(SUM(duration_sec), 0), 3) AS release_date_quarter_to_duration_sec_ratio,
        ROUND(100 * COUNT(CASE WHEN NOT is_explicit THEN 1 END) / COUNT(*), 1) AS pct_not_is_explicit
    FROM recent_tracks
    WHERE artist_id IS NOT NULL
    GROUP BY is_explicit, artist_id, release_date_quarter
    HAVING AVG(duration_sec) > 164
),
without_matches AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM rollup_base s
    WHERE s.artist_id IN (
            SELECT t.artist_id
            FROM tracks t
            WHERE t.bpm < 142
              AND t.duration_sec > 142
        )
),
with_activity AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM without_matches s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.is_explicit = FALSE
              AND t.duration_sec BETWEEN 142 AND 392
        )
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        COUNT(*) FILTER (WHERE is_explicit = TRUE) AS track_is_explicit_count,
        COUNT(CASE WHEN is_explicit = FALSE THEN 1 END) AS track_is_explicit_count_2
    FROM tracks
    GROUP BY artist_id
),
with_counts AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        COALESCE(t.track_is_explicit_count, 0) AS track_is_explicit_count,
        COALESCE(t.track_is_explicit_count_2, 0) AS track_is_explicit_count_2
    FROM with_activity s
    LEFT JOIN track_stats t ON t.artist_id = s.artist_id
)
SELECT
    is_explicit,
    stddev_release_date_quarter,
    release_date_quarter_to_duration_sec_ratio,
    pct_not_is_explicit,
    track_is_explicit_count
FROM with_counts
ORDER BY pct_not_is_explicit
