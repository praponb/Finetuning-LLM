WITH recent AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
        LENGTH(TRIM(title)) AS title_len,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec
    FROM tracks
    WHERE NOT is_explicit
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        title_slug,
        title_len,
        bpm_per_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), artist_id ORDER BY bpm_per_duration_sec, id) AS rn,
        FIRST_VALUE(bpm_per_duration_sec) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), artist_id ORDER BY bpm_per_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm_per_duration_sec
    FROM recent
),
flattened AS (
    -- One row per mood
    SELECT
        s.id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.title_slug,
        s.title_len,
        s.bpm_per_duration_sec,
        s.rn,
        s.first_known_bpm_per_duration_sec,
        t.mood
    FROM sequenced s
    LATERAL VIEW OUTER EXPLODE(s.moods) t AS mood
)
-- Final projection
SELECT
    id,
    title,
    duration_sec,
    release_date,
    is_explicit,
    title_slug,
    title_len,
    bpm_per_duration_sec,
    rn,
    first_known_bpm_per_duration_sec,
    mood,
    ROUND(duration_sec / NULLIF(title_len, 0), 3) AS duration_sec_per_title_len,
    CONCAT(title_slug, '#', CAST(duration_sec AS STRING)) AS title_slug_ref
FROM flattened
ORDER BY id NULLS LAST, first_known_bpm_per_duration_sec, mood
LIMIT 20
