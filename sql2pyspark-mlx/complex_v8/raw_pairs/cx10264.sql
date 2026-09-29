WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec,
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix
    FROM tracks
    WHERE release_date < DATE '2025-08-02'
      AND artist_id <= 13
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        duration_sec_per_bpm,
        title_prefix,
        LAG(bpm) OVER (PARTITION BY is_explicit ORDER BY bpm DESC, id) AS prev_bpm,
        ROUND(CUME_DIST() OVER (PARTITION BY is_explicit ORDER BY bpm DESC), 3) AS bpm_cume_dist,
        MAX(bpm_per_duration_sec) OVER (PARTITION BY artist_id) AS artist_id_max_bpm_per_duration_sec,
        LAST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY is_explicit ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec
    FROM valid
),
without_matches AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM sequenced s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.device IN ('speaker', 'car')
        )
),
filtered AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM without_matches s
    WHERE s.id IN (
            SELECT p.track_id
            FROM plays p
        )
)
SELECT id, field, value
FROM (
    SELECT id, prev_bpm, bpm, last_known_duration_sec
    FROM filtered
) src
UNPIVOT INCLUDE NULLS (
    value FOR field IN (prev_bpm, bpm, last_known_duration_sec)
) unp
