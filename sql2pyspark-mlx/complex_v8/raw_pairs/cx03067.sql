WITH scoped_tracks AS (
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
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        MAX(release_date) AS last_track_release_date,
        SUM(duration_sec) AS total_track_duration_sec
    FROM tracks
    GROUP BY artist_id
),
with_counts AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        t.last_track_release_date,
        t.total_track_duration_sec
    FROM scoped_tracks s
    LEFT JOIN track_stats t ON t.artist_id = s.artist_id
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        is_explicit,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT artist_id) AS distinct_artist_ids,
        SUM(CASE WHEN UPPER(TRIM(title)) LIKE 'G%' THEN bpm ELSE 0 END) AS bpm_upper_trim_title_like_g,
        MIN(bpm) AS min_bpm,
        MAX(duration_sec) AS max_duration_sec
    FROM with_counts
    GROUP BY is_explicit
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(bpm_upper_trim_title_like_g - AVG(bpm_upper_trim_title_like_g) OVER (PARTITION BY n_rows, is_explicit), 2) AS bpm_upper_trim_title_like_g_vs_n_rows_is_explici,
        SUM(min_bpm) OVER (PARTITION BY is_explicit, distinct_artist_ids ORDER BY distinct_ids, is_explicit ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_bpm,
        DENSE_RANK() OVER (PARTITION BY distinct_artist_ids, is_explicit ORDER BY max_duration_sec, is_explicit) AS max_duration_sec_dense_rank
    FROM rollup_base
)
-- Subtotals with GROUPING SETS
SELECT
    distinct_artist_ids,
    n_rows,
    SUM(distinct_ids) AS total_distinct_ids,
    MAX(distinct_artist_ids) AS max_distinct_artist_ids,
    COUNT(*) FILTER (WHERE is_explicit = TRUE) AS n_is_explicit_true
FROM sequenced
GROUP BY GROUPING SETS ((distinct_artist_ids, n_rows), (distinct_artist_ids), ())
ORDER BY distinct_artist_ids NULLS LAST, n_rows NULLS LAST
