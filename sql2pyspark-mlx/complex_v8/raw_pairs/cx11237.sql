WITH filtered AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods
    FROM tracks
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        bpm,
        moods,
        COALESCE(bpm, 0) AS bpm_filled
    FROM filtered
    WHERE duration_sec >= 165
),
qualified AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM labeled s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.bpm < 155
              AND t.is_explicit = TRUE
        )
),
rollup_base AS (
    -- Group-level totals
    SELECT
        artist_id,
        SUM(bpm) AS total_bpm,
        MAX(bpm) AS max_bpm,
        PERCENTILE_APPROX(bpm_filled, 0.5) AS median_bpm_filled,
        COUNT(DISTINCT artist_id) AS distinct_artist_ids,
        COUNT(*) AS row_count
    FROM qualified
    GROUP BY 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT artist_id, metric, amount
FROM (
    SELECT artist_id, row_count, total_bpm, distinct_artist_ids
    FROM rollup_base
) src
UNPIVOT (
    amount FOR metric IN (row_count, total_bpm, distinct_artist_ids)
) unp
ORDER BY artist_id, metric
