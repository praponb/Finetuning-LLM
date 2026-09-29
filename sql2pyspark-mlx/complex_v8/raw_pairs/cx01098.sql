WITH prepared AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        ABS(duration_sec - 279) AS duration_sec_distance,
        ADD_MONTHS(release_date, 12) AS release_date_plus_12m
    FROM tracks
    WHERE NOT is_explicit
),
summary AS (
    -- Summarise per group
    SELECT
        artist_id,
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_ym,
        DATE_FORMAT(release_date_plus_12m, 'yyyy-MM') AS release_date_plus_12m_ym,
        SUM(CASE WHEN is_explicit = TRUE THEN 1 ELSE 0 END) AS n_is_explicit_true,
        ARRAY_SORT(COLLECT_SET(title)) AS title_list,
        COUNT(*) AS n_rows,
        ROUND(AVG(bpm), 2) AS avg_bpm,
        COUNT(DISTINCT id) AS distinct_ids
    FROM prepared
    GROUP BY 1, 2, 3
)
SELECT
    artist_id,
    release_date_plus_12m_ym,
    avg_bpm,
    distinct_ids,
    CONCAT(release_date_ym, '#', CAST(n_is_explicit_true AS STRING)) AS release_date_ym_ref,
    LOWER(REGEXP_REPLACE(TRIM(release_date_ym), ' ', '_')) AS release_date_ym_slug
FROM summary
ORDER BY release_date_ym_slug DESC NULLS FIRST, release_date_ym_ref
