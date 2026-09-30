WITH recent_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        DATE_TRUNC('QUARTER', release_date) AS release_date_quarter,
        CONCAT(title, '#', CAST(id AS STRING)) AS title_ref
    FROM tracks
    WHERE (release_date < DATE '2025-08-02' OR title IS NOT NULL)
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        title,
        is_explicit,
        release_date_quarter,
        title_ref,
        LOWER(TRIM(title)) AS title_clean,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label,
        HOUR(release_date_quarter) AS release_date_quarter_hour
    FROM recent_tracks
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        SUBSTRING(TRIM(title), 1, 3) AS title_prefix,
        DATE_TRUNC('MONTH', release_date_quarter) AS release_date_quarter_month
    FROM annotated
),
filtered AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.is_skipped = TRUE
        )
)
SELECT
    is_explicit,
    is_explicit_label,
    release_date_quarter_hour,
    MAX(release_date_quarter_hour) AS max_release_date_quarter_hour,
    ROUND(100 * COUNT(CASE WHEN title IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_title_is_not_null,
    ARRAY_SORT(COLLECT_SET(title)) AS title_list
FROM filtered
GROUP BY CUBE(is_explicit, is_explicit_label, release_date_quarter_hour)
ORDER BY is_explicit NULLS LAST, is_explicit_label NULLS LAST, release_date_quarter_hour NULLS LAST
