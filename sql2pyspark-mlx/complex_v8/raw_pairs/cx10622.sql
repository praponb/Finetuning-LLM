WITH cleaned_plays AS (
    -- Scope the plays rows
    SELECT *
    FROM plays
    WHERE device = 'car'
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        ABS(ms_played - 112786) AS ms_played_distance,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM cleaned_plays
    WHERE is_skipped = TRUE
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        is_skipped,
        track_id,
        source,
        SUM(ms_played) AS total_ms_played,
        SUM(CASE WHEN is_skipped = TRUE THEN 1 ELSE 0 END) AS n_is_skipped_true,
        MIN(played_at) AS first_played_at
    FROM scored
    GROUP BY 1, 2, 3
    HAVING AVG(ms_played_bucket) > 123500
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        is_skipped,
        track_id,
        source,
        first_played_at,
        COUNT(*) OVER (PARTITION BY n_is_skipped_true) AS rows_in_n_is_skipped_true,
        FIRST_VALUE(n_is_skipped_true) OVER (PARTITION BY track_id, source ORDER BY n_is_skipped_true ASC, is_skipped, track_id, source ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_n_is_skipped_true,
        RANK() OVER (ORDER BY n_is_skipped_true DESC NULLS FIRST, is_skipped, track_id, source) AS n_is_skipped_true_rank
    FROM totals
),
with_details_track AS (
    -- Join tracks
    SELECT
        s.is_skipped,
        s.track_id,
        s.source,
        s.rows_in_n_is_skipped_true,
        s.first_n_is_skipped_true,
        s.n_is_skipped_true_rank,
        t.artist_id AS track_artist_id,
        t.moods AS track_moods,
        t.duration_sec AS track_duration_sec
    FROM with_trends s
    JOIN tracks t ON t.id = s.track_id
),
scored_2 AS (
    -- Derive helper columns
    SELECT
        *,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label,
        SUBSTRING(TRIM(source), 1, 3) AS source_prefix,
        CONCAT(source, '#', CAST(track_id AS STRING)) AS source_ref
    FROM with_details_track
    WHERE SIZE(track_moods) >= 2
),
is_skipped_label_stats AS (
    -- Max rows_in_n_is_skipped_true per is_skipped_label
    SELECT is_skipped_label, MAX(rows_in_n_is_skipped_true) AS max_rows_in_n_is_skipped_true
    FROM scored_2
    GROUP BY is_skipped_label
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_rows_in_n_is_skipped_true,
        ROUND(s.rows_in_n_is_skipped_true - g.max_rows_in_n_is_skipped_true, 2) AS rows_in_n_is_skipped_true_minus_max
    FROM scored_2 s
    JOIN is_skipped_label_stats g ON g.is_skipped_label = s.is_skipped_label
    WHERE s.rows_in_n_is_skipped_true = g.max_rows_in_n_is_skipped_true
)
-- Final projection
SELECT
    is_skipped,
    track_id,
    source,
    rows_in_n_is_skipped_true,
    first_n_is_skipped_true,
    track_moods,
    track_duration_sec,
    is_skipped_label,
    source_ref,
    max_rows_in_n_is_skipped_true,
    LENGTH(TRIM(source)) AS source_len
FROM above_average
WHERE source IS NOT NULL
ORDER BY track_id, is_skipped, source
LIMIT 20
