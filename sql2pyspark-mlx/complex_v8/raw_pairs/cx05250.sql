WITH prepared AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        SIZE(moods) AS moods_count,
        LENGTH(TRIM(title)) AS title_len,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket
    FROM tracks
    WHERE artist_id IS NOT NULL
),
per_label AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods_count,
        s.title_len,
        s.duration_sec_bucket,
        t.mood
    FROM prepared s
    LATERAL VIEW EXPLODE(s.moods) t AS mood
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        is_explicit,
        ROUND(100 * COUNT(CASE WHEN title IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_title_is_not_null,
        DATEDIFF(MAX(release_date), MIN(release_date)) AS release_date_span_days
    FROM per_label
    GROUP BY is_explicit
),
labeled AS (
    -- Add calculated columns
    SELECT
        is_explicit,
        pct_title_is_not_null,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag
    FROM stats
    WHERE is_explicit = FALSE
)
-- Final projection
SELECT
    is_explicit,
    pct_title_is_not_null,
    is_explicit_flag,
    ABS(is_explicit_flag - 0) AS is_explicit_flag_distance,
    pct_title_is_not_null - is_explicit_flag AS pct_title_is_not_null_minus_is_explicit_flag
FROM labeled
