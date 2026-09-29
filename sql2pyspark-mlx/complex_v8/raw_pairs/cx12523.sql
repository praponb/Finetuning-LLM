WITH valid_tracks AS (
    -- Filter tracks
    SELECT *
    FROM tracks
    WHERE duration_sec <= 357
      AND release_date < DATE '2025-04-02'
),
filtered AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM valid_tracks s
    WHERE NOT EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND a.country = 'JP'
        )
),
filtered_2 AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.listener_id <= 6
        )
),
per_group AS (
    -- Summarise per group
    SELECT
        is_explicit,
        COUNT(*) AS cnt,
        SUM(CASE WHEN is_explicit = TRUE THEN bpm ELSE 0 END) AS bpm_is_explicit_true
    FROM filtered_2
    WHERE release_date IS NOT NULL
    GROUP BY 1
)
-- Final projection
SELECT
    is_explicit,
    cnt,
    bpm_is_explicit_true,
    GREATEST(cnt, bpm_is_explicit_true) AS greatest_cnt_bpm_is_explicit_true,
    cnt - bpm_is_explicit_true AS cnt_minus_bpm_is_explicit_true
FROM per_group
ORDER BY cnt NULLS LAST, greatest_cnt_bpm_is_explicit_true DESC
