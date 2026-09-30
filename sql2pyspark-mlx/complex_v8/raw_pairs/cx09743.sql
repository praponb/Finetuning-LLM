WITH cleaned_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        FLOOR(bpm / 25) * 25 AS bpm_bucket,
        CONCAT(title, '#', CAST(bpm AS STRING)) AS title_ref,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix
    FROM tracks
    WHERE NOT is_explicit
      AND bpm <= 142
),
enriched AS (
    -- Join artists
    SELECT s.*, a.genre AS artist_genre
    FROM cleaned_tracks s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.genre = 'Classical'
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_ym,
        SUM(duration_sec) FILTER (WHERE is_explicit = TRUE) AS duration_sec_is_explicit_true,
        SUM(CASE WHEN artist_genre = 'Jazz' THEN bpm_bucket ELSE 0 END) AS bpm_bucket_artist_genre_jazz,
        ARRAY_SORT(COLLECT_SET(title)) AS title_list,
        PERCENTILE_APPROX(bpm_bucket, 0.5) AS median_bpm_bucket
    FROM enriched
    GROUP BY DATE_FORMAT(release_date, 'yyyy-MM')
    HAVING SUM(duration_sec) > 164
),
derived AS (
    -- Label and score rows
    SELECT
        release_date_ym,
        duration_sec_is_explicit_true,
        bpm_bucket_artist_genre_jazz,
        median_bpm_bucket,
        COALESCE(median_bpm_bucket, 0) AS median_bpm_bucket_filled,
        LENGTH(TRIM(release_date_ym)) AS release_date_ym_len
    FROM grouped
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM derived
    WHERE bpm_bucket_artist_genre_jazz > (SELECT AVG(bpm_bucket_artist_genre_jazz) FROM derived)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        release_date_ym,
        duration_sec_is_explicit_true,
        median_bpm_bucket,
        median_bpm_bucket_filled,
        release_date_ym_len,
        LAG(release_date_ym_len) OVER w_release_date_ym_len_by_media AS prev_release_date_ym_len,
        ROUND(AVG(bpm_bucket_artist_genre_jazz) OVER (PARTITION BY release_date_ym_len), 2) AS release_date_ym_len_avg_bpm_bucket_artist_genre_,
        COUNT(*) OVER (PARTITION BY release_date_ym_len) AS rows_in_release_date_ym_len,
        LAG(release_date_ym_len) OVER (PARTITION BY release_date_ym_len ORDER BY duration_sec_is_explicit_true, release_date_ym) AS prev_release_date_ym_len_2,
        ROW_NUMBER() OVER (PARTITION BY release_date_ym_len ORDER BY median_bpm_bucket_filled DESC NULLS FIRST, release_date_ym) AS rn
    FROM with_activity
    WINDOW w_release_date_ym_len_by_media AS (PARTITION BY release_date_ym_len ORDER BY median_bpm_bucket_filled DESC NULLS FIRST, release_date_ym)
)
SELECT
    release_date_ym,
    duration_sec_is_explicit_true,
    median_bpm_bucket_filled,
    release_date_ym_len,
    release_date_ym_len_avg_bpm_bucket_artist_genre_,
    rows_in_release_date_ym_len,
    prev_release_date_ym_len_2,
    rn,
    median_bpm_bucket_filled - prev_release_date_ym_len AS median_bpm_bucket_filled_minus_prev_release_date,
    CASE WHEN median_bpm_bucket_filled >= 138 THEN 'large' WHEN median_bpm_bucket_filled >= 102 THEN 'medium' ELSE 'small' END AS median_bpm_bucket_filled_tier
FROM ranked
WHERE prev_release_date_ym_len_2 < 12
ORDER BY release_date_ym_len NULLS LAST, release_date_ym
LIMIT 20
