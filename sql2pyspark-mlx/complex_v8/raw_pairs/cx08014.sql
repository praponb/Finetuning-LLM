WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm
    FROM tracks
    WHERE is_explicit = TRUE
),
unique_rows AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY release_date DESC, id DESC) AS row_num
        FROM scoped
    ) ranked
    WHERE row_num = 1
),
qualified AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM unique_rows s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.device IS NOT NULL
              AND p.ms_played >= 107912
        )
)
-- Subtotals with GROUPING SETS
SELECT
    is_explicit,
    artist_id,
    GROUPING(is_explicit) AS is_total_is_explicit,
    SUM(bpm) AS total_bpm,
    MAX(release_date) AS last_release_date
FROM qualified
GROUP BY GROUPING SETS ((is_explicit, artist_id), (is_explicit), (artist_id), ())
ORDER BY is_explicit NULLS LAST, artist_id NULLS LAST
