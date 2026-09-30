WITH filtered_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        FLOOR(bpm / 25) * 25 AS bpm_bucket,
        CONCAT(title, '#', CAST(artist_id AS STRING)) AS title_ref
    FROM tracks
    WHERE is_explicit = TRUE
),
with_parent AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        s.bpm_bucket,
        s.title_ref,
        a.country AS artist_country
    FROM filtered_tracks s
    JOIN artists a ON a.id = s.artist_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(bpm) OVER w_artist_country, 2) AS artist_country_avg_bpm,
        SUM(bpm_bucket) OVER (ORDER BY duration_sec NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm_bucket,
        ROW_NUMBER() OVER (PARTITION BY is_explicit ORDER BY bpm, id) AS seq_no,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER w_artist_country, 0), 4) AS duration_sec_share_of_artist_country,
        MAX(bpm_bucket) OVER w_artist_country AS artist_country_max_bpm_bucket,
        LEAD(duration_sec, 3) OVER (ORDER BY duration_sec NULLS LAST, id) AS next3_duration_sec
    FROM with_parent
    WINDOW w_artist_country AS (PARTITION BY artist_country)
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        title,
        is_explicit,
        bpm,
        bpm_bucket,
        artist_country_avg_bpm,
        running_bpm_bucket,
        duration_sec_share_of_artist_country,
        artist_country_max_bpm_bucket,
        next3_duration_sec,
        LOWER(REGEXP_REPLACE(TRIM(title_ref), ' ', '_')) AS title_ref_slug,
        next3_duration_sec - bpm_bucket AS next3_duration_sec_minus_bpm_bucket
    FROM sequenced
    WHERE duration_sec_share_of_artist_country < 0.8
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        title,
        is_explicit,
        bpm_bucket,
        running_bpm_bucket,
        duration_sec_share_of_artist_country,
        next3_duration_sec,
        next3_duration_sec_minus_bpm_bucket,
        RANK() OVER w_is_explicit_by_bpm_ties AS bpm_rank,
        FIRST_VALUE(running_bpm_bucket) IGNORE NULLS OVER (PARTITION BY is_explicit ORDER BY running_bpm_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_bpm_bucket,
        COUNT(*) OVER (PARTITION BY is_explicit) AS rows_in_is_explicit
    FROM scored
    WINDOW w_is_explicit_by_bpm_ties AS (PARTITION BY is_explicit ORDER BY bpm)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        title,
        is_explicit,
        bpm_bucket,
        running_bpm_bucket,
        duration_sec_share_of_artist_country,
        next3_duration_sec,
        next3_duration_sec_minus_bpm_bucket,
        first_known_running_bpm_bucket,
        rows_in_is_explicit
    FROM metrics
    WHERE bpm_rank = 1
      AND next3_duration_sec >= 239
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        title,
        running_bpm_bucket,
        next3_duration_sec_minus_bpm_bucket,
        GREATEST(bpm_bucket, next3_duration_sec) AS greatest_bpm_bucket_next3_duration_sec,
        CASE WHEN next3_duration_sec >= 312 THEN 'HIGH' WHEN next3_duration_sec >= 164 THEN 'MEDIUM' ELSE 'LOW' END AS next3_duration_sec_tier
    FROM leaders
    WHERE next3_duration_sec_minus_bpm_bucket <= 546
)
SELECT
    id,
    running_bpm_bucket,
    next3_duration_sec_minus_bpm_bucket,
    FLOOR(running_bpm_bucket / 250) * 250 AS running_bpm_bucket_bucket
FROM annotated
ORDER BY running_bpm_bucket_bucket DESC, id
LIMIT 5
