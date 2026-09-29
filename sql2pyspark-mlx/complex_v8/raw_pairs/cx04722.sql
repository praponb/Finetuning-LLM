WITH cleaned_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit
    FROM tracks
    WHERE duration_sec > 183
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        COUNT(*) AS track_count,
        COUNT(*) AS track_count_2,
        MAX(release_date) AS last_track_release_date
    FROM tracks
    GROUP BY artist_id
),
with_counts AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        COALESCE(t.track_count, 0) AS track_count,
        COALESCE(t.track_count_2, 0) AS track_count_2,
        t.last_track_release_date
    FROM cleaned_tracks s
    LEFT JOIN track_stats t ON t.artist_id = s.artist_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(track_count_2 - AVG(track_count_2) OVER (PARTITION BY track_count, artist_id), 2) AS track_count_2_vs_track_count_artist_id_avg,
        RANK() OVER (ORDER BY track_count) AS track_count_rank,
        ROUND(CUME_DIST() OVER w_all_by_track_count_ties, 3) AS track_count_cume_dist,
        NTILE(4) OVER w_all_by_track_count AS track_count_quartile
    FROM with_counts
    WINDOW w_all_by_track_count_ties AS (ORDER BY track_count), w_all_by_track_count AS (ORDER BY track_count, id)
)
SELECT
    is_explicit,
    track_count,
    COUNT(DISTINCT artist_id) AS distinct_artist_ids,
    SUM(duration_sec) AS total_duration_sec
FROM metrics
GROUP BY GROUPING SETS ((is_explicit, track_count), (is_explicit))
ORDER BY is_explicit NULLS LAST, track_count NULLS LAST
