WITH prepared AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug_2,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label
    FROM tracks
    WHERE LENGTH(TRIM(title)) >= 6
      AND ARRAY_CONTAINS(moods, 'chill')
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        moods,
        title_slug,
        title_slug_2,
        is_explicit_label,
        INITCAP(TRIM(title)) AS title_clean,
        SIZE(moods) AS moods_count
    FROM prepared
),
filtered AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM labeled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.is_skipped = FALSE
              AND p.ms_played > 42041
        )
      AND LENGTH(TRIM(s.title)) >= 6
),
unique_rows AS (
    -- Keep the earliest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        moods,
        title_slug,
        title_slug_2,
        is_explicit_label,
        title_clean,
        moods_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY moods_count, id) AS recency_rank
        FROM filtered
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    artist_id,
    moods_count,
    is_explicit,
    GROUPING_ID() AS grouping_level,
    SUM(moods_count) AS total_moods_count
FROM unique_rows
GROUP BY ROLLUP(artist_id, moods_count, is_explicit)
ORDER BY artist_id NULLS LAST, moods_count NULLS LAST, is_explicit NULLS LAST
