WITH base AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        ABS(bpm - 100) AS bpm_distance,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label
    FROM tracks
    WHERE YEAR(release_date) = 2024
),
without_matches AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.duration_sec >= 142
              AND ARRAY_CONTAINS(t.moods, 'party')
        )
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        artist_id,
        is_explicit,
        bpm,
        moods,
        bpm_distance,
        is_explicit_label,
        CONCAT_WS('-', title, is_explicit_label) AS title_is_explicit_label_label,
        LOWER(REGEXP_REPLACE(TRIM(is_explicit_label), ' ', '_')) AS is_explicit_label_slug,
        QUARTER(release_date) AS release_date_quarter
    FROM without_matches
    WHERE is_explicit = FALSE
)
-- Subtotals with ROLLUP
SELECT
    is_explicit,
    artist_id,
    SUM(bpm) AS total_bpm
FROM scored
GROUP BY ROLLUP(is_explicit, artist_id)
ORDER BY is_explicit NULLS LAST, artist_id NULLS LAST
