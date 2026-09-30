WITH prepared_tracks AS (
    -- Keep valid rows and derive helper columns
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
    WHERE duration_sec BETWEEN 165 AND 352
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        LAST_VALUE(bpm) OVER (PARTITION BY is_explicit ORDER BY duration_sec NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_bpm,
        ROW_NUMBER() OVER (PARTITION BY is_explicit ORDER BY duration_sec NULLS LAST, id) AS duration_sec_seq,
        COUNT(*) OVER (PARTITION BY is_explicit) AS rows_in_is_explicit,
        DENSE_RANK() OVER w_is_explicit_by_duration_sec_2 AS duration_sec_dense_rank
    FROM prepared_tracks
    WINDOW w_is_explicit_by_duration_sec_2 AS (PARTITION BY is_explicit ORDER BY duration_sec NULLS LAST, id)
),
without_matches AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM windowed s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
        )
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        release_date,
        is_explicit,
        bpm,
        moods,
        duration_sec_seq,
        duration_sec_dense_rank,
        LEAST(duration_sec, rows_in_is_explicit) AS least_duration_sec_rows_in_is_explicit
    FROM without_matches
    WHERE rows_in_is_explicit > 10
)
-- Subtotals with GROUPING SETS
SELECT
    duration_sec_seq,
    duration_sec_dense_rank,
    GROUPING_ID() AS gid,
    ROUND(AVG(duration_sec_dense_rank), 2) AS avg_duration_sec_dense_rank
FROM derived
GROUP BY GROUPING SETS ((duration_sec_seq, duration_sec_dense_rank), (duration_sec_seq), (duration_sec_dense_rank))
ORDER BY duration_sec_seq NULLS LAST, duration_sec_dense_rank NULLS LAST
